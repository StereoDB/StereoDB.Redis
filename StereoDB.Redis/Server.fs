module StereoDB.Redis.Server

open System.Net
open System.Net.Sockets
open System.Threading.Tasks
open System.IO
open System.Threading
open StereoDB
open StereoDB.FSharp
open StereoDB.Redis.Schema
open System.Text

type ClientConnection = {
        Id: int
        CancellationTokenSource : CancellationTokenSource
        WorkerTask: Task
        Stream: NetworkStream
        mutable Name: string
        mutable LibraryName: string
        mutable LibraryVersion: string
    }

let mutable clientTaskList : ClientConnection list = []

let addClientTask cl = clientTaskList <- cl :: clientTaskList

let findClientConnection clientId = clientTaskList |> List.find (fun conn -> conn.Id = clientId)

let removeFirst pred list = 
    let rec removeFirstTailRec p l acc =
        match l with
        | [] -> acc |> List.rev
        | h::t when p h -> (acc |> List.rev) @ t
        | h::t -> removeFirstTailRec p t (h::acc)
    removeFirstTailRec pred list []

type RedisValue = 
    | Nil
    | String of string
    | Number of int64
    | Error of string
    | Ok
    | Array of RedisValue array

let rec serializeValue value =
    match value with
    | Nil -> "$-1"
    | String v -> "$" + v.Length.ToString() + "\r\n" + v
    | Number v -> ":" + string v
    | Error v -> "-ERR " + v
    | Ok -> "+OK"
    | Array v -> "*" + string v.Length + "\r\n" + System.String.Join("\r\n", v |> Array.map serializeValue)

let writeToClient cl (msg: string) =
    let streamWriter = new StreamWriter(stream= cl)
    task {
        do! streamWriter.WriteLineAsync(msg)
        do! streamWriter.FlushAsync()
    }

let db = StereoDb.create(Schema(), StereoDbSettings.Default)

let handleResponse clientId (line: string array) =
    let response = 
        match line with
        | [|"GET"|] ->  Error "Wrong number of arguments for 'get' command"
        | [|"GET"; key |] ->
            let result = db.ReadTransaction (fun ctx ->
                let records = ctx.UseTable(ctx.Schema.Records.Table)
                records.Get key
            )
            match result with
            | ValueNone -> Nil
            | ValueSome value -> String value.Value
        | [|"CLIENT"; "SETNAME"|] -> Error "Wrong number of arguments for 'SETNAME' command"
        | [|"CLIENT"; "SETNAME"; name|] -> 
            let connection = findClientConnection clientId
            connection.Name <- name
            Ok
        | [|"INCR"|] -> Error "Wrong number of arguments for 'incr' command"
        | [|"INCR"; key |] ->
            let mutable result = 
                db.WriteTransaction (fun ctx ->
                    let records = ctx.UseTable(ctx.Schema.Records.Table)
                    let value = records.Get key
                    let result =
                        match value with
                        | ValueNone -> 1L
                        | ValueSome value -> (int64 value.Value) + 1L
                    records.Set { Id=key; Value = string result }
                    ValueSome result
                )
            match result with
            | ValueNone -> Error (System.String.Format("Increment of key {0} failed", key))
            | ValueSome value -> Number value
        | [|"PING"|] -> String "PONG"
        | [|"PING"; message|] -> String message
        | [|"CLIENT"; "SETINFO"|] -> Error "Wrong number of arguments for 'SETINFO' command"
        | [|"CLIENT"; "SETINFO"; "lib-ver"; libVer|] -> 
            let connection = findClientConnection clientId
            connection.LibraryVersion <- libVer
            Ok
        | [|"CLIENT"; "SETINFO"; "lib-name"; libName|] -> 
            let connection = findClientConnection clientId
            connection.LibraryName <- libName
            Ok
        | [|"CONFIG"; "GET"; configName|] -> 
            match configName with
            | "slave-read-only" -> String "yes"
            // Snapshotting parameter https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/#snapshotting
            | "save" -> Array [| String "SAVE";String "3600 1 300 100 60 10000" |]
            // Append only parameter https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/#append-only-file
            | "appendonly" -> Array [| String "appendonly";String "no" |]
            | "databases" -> Array [||]
            | _ -> Error $"Unknown configuration parameter '%s{configName}'"
        | [|"CLIENT"; command|] -> Error $"unknown command '%s{command}'"
        | [|"ECHO"; message |] -> String message
        | [|"SET"|] -> Error "Wrong number of arguments for 'set' command"
        | [|"SET"; _ |] -> Error "Wrong number of arguments for 'set' command"
        | [|"SET"; key; value |] -> 
            db.WriteTransaction (fun ctx ->
                let records = ctx.UseTable(ctx.Schema.Records.Table)
                records.Set { Id=key; Value = value}
            )
            Ok
        | _  -> Error $"Invalid command %A{line}"
    response
    
let parseLine () =
    let mutable expected = 0
    let x = 
        fun (line: string) ->
            Some(line)
    x
let processor () =
    let mutable stack = [||]
    let mutable expected = 0
    let mutable currentType = None
    let worker = 
        fun (line: string) ->
            if line.StartsWith "*" then
                stack <- [||]
                expected <- int (line.Substring(1))
                currentType <- None
                None
            else
                if expected = 0 && (currentType.IsNone) then
                    Some (line.Split(' ', System.StringSplitOptions.RemoveEmptyEntries))
                else
                    let currentObj =
                        match currentType with 
                        | None -> 
                            if (line.StartsWith("$")) then
                                let currentParser = parseLine ()
                                currentParser line |> ignore
                                currentType <- Some (currentParser)
                                None
                            else
                                Some line
                        | Some parser ->
                            match parser line with
                            | None -> None
                            | Some obj -> 
                                currentType <- None
                                Some obj
                    match currentObj with
                    | Some currentObj -> 
                        expected <- expected - 1
                        stack <- Array.append stack [| currentObj |]
                        if (expected = 0) then
                            Some stack
                        else
                            None
                    | None -> None
    worker

let trace (message : string) =
#if DEBUG
    System.Console.WriteLine(message)
#endif
    ()

let listenForMessages clientId endpoint stream = 
    let listenWorkflow = 
        task { 
            use reader = new System.IO.StreamReader(stream = stream)
            try 
                let mutable continueListening = true
                let builder = StringBuilder()
                let buffer = Array.create 1024 ' '
                let processor = processor()
                while continueListening do
                    let! readCount = reader.ReadAsync(buffer, 0, 1024)
                    if readCount = 0 then continueListening <- false
                    for i = 0 to readCount - 1 do
                        if (buffer[i] = '\n' && builder.Length > 0 && builder[builder.Length - 1] = '\r') then
                            builder.Remove(builder.Length - 1, 1) |> ignore
                            let line = builder.ToString()
                            trace $"%d{clientId} >> %s{line}"
                            builder.Clear() |> ignore
                            match processor line with
                            | None -> ()
                            | Some command ->                                
                                let response = handleResponse clientId command
                                let line = response |> serializeValue
                                trace $"%d{clientId} << %s{line}"
                                do! writeToClient stream line
                        else
                            builder.Append(buffer[i]) |> ignore
            with _ -> let client = findClientConnection clientId
                      let { Stream = stream; CancellationTokenSource = cts; WorkerTask = task } = client

                      cts.Cancel()
                      task.Dispose()
                      stream.Dispose()

                      clientTaskList <- clientTaskList |> removeFirst (fun conn -> conn.Id = clientId)
                      printfn $"%s{endpoint} disconnected"
        }
    listenWorkflow

let listen port = 
    let mutable id = 0
    let listenWorkflow =  
        task { 
            let listener = new TcpListener(IPAddress.Any, port)
            listener.Start()
            printfn $"Start listening on port %i{port}"
            while true do
                let! client = listener.AcceptTcpClientAsync() |> Async.AwaitTask
                let endpoint = (client.Client.RemoteEndPoint :?> IPEndPoint).Address.ToString()

                id <- id + 1
                printfn $"Client connected: %A{endpoint}, id: %d{id}"

                let cts = new CancellationTokenSource();
                let clientListenTask = Task.Run (fun () -> listenForMessages id endpoint (client.GetStream()), cts.Token)
                addClientTask { Id = id; CancellationTokenSource = cts; WorkerTask = clientListenTask; Stream = client.GetStream(); Name = ""; LibraryName = ""; LibraryVersion = "" }
        }
    listenWorkflow
