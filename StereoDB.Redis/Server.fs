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

type ClientConnect = {
        Id: int
        CancellationTokenSource : CancellationTokenSource
        WorkerTask: Task
        Stream: NetworkStream
    }

let mutable clientTaskList : ClientConnect list = []

let addClientTask cl = clientTaskList <- cl :: clientTaskList

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
    | Error of string
    | Ok
    | Array of RedisValue list

let rec serializeValue value =
    match value with
    | Nil -> "$-1"
    | String v -> v.Length.ToString() + "\r\n" + v
    | Error v -> "-ERR " + v
    | Ok -> "+OK"
    | Array v -> "*" + string v.Length + "\r\n" + System.String.Join("\r\n", v |> List.map serializeValue)

let writeToClient cl (msg: string) =
    let streamWriter = new StreamWriter(stream= cl)
    task {
        do! streamWriter.WriteLineAsync(msg)
        do! streamWriter.FlushAsync()
    }

let db = StereoDb.create(Schema(), StereoDbSettings.Default)

let handleResponse (line: string array) =
    let response = 
        match line with
        | [|"GET"|] ->  Error "wrong number of arguments for 'get' command"
        | [|"GET"; key |] ->
            let result = db.ReadTransaction (fun ctx ->
                let records = ctx.UseTable(ctx.Schema.Records.Table)
                records.Get key
            )
            match result with
            | ValueNone -> Nil
            | ValueSome value -> String value.Value
        | [|"SET"|] -> Error "wrong number of arguments for 'set' command"
        | [|"CLIENT"; "SETNAME"|] -> Error "wrong number of arguments for 'SETNAME' command"
        | [|"CLIENT"; "SETNAME"; name|] -> Ok
        | [|"CLIENT"; "SETINFO"|] -> Error "wrong number of arguments for 'SETINFO' command"
        | [|"CLIENT"; "SETINFO"; "lib-ver"; libVer|] -> Ok
        | [|"CLIENT"; "SETINFO"; "lib-name"; libName|] -> Ok
        | [|"CONFIG"; "GET"; configName|] -> 
            match configName with
            | "slave-read-only" -> String "yes"
            | "databases" -> Array []
            | _ -> Error (sprintf "unknown configuration parameter '%s'" configName)
        | [|"CLIENT"; command|] -> Error (sprintf "unknown command '%s'" command)
        | [|"ECHO"; message |] -> String message
        | [|"SET"; _ |] -> Error "wrong number of arguments for 'set' command"
        | [|"SET"; key; value |] -> 
            db.WriteTransaction (fun ctx ->
                let records = ctx.UseTable(ctx.Schema.Records.Table)
                records.Set { Id=key; Value = value}
            )
            Ok
        | _  -> Error (sprintf "Invalid command %A" line)
    response
    
let handleInlineCommand (line: string) =
    let line = line.Split(' ', System.StringSplitOptions.RemoveEmptyEntries)
    handleResponse line

let decodeArray (stack: string array) =
    let s = 
        seq {
            let mutable size = None
            for i = 0 to stack.Length do
                match size with
                | None -> size <- Some (int stack[i])
                | Some size -> 
                    assert (stack[i].Length = size)
                    yield stack[i]
        }
    s |> Seq.toArray

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

let listenForMessages clientId endpoint cl = 
    let listenWorkflow = 
        task { 
            use reader = new System.IO.StreamReader(stream = cl)
            try 
                let mutable continueListening = true
                let builder = StringBuilder()
                let buffer = Array.create 1024 ' '
                let processor = processor()
                while continueListening do
                    let! readCount = reader.ReadAsync(buffer, 0, 1024)
                    for i = 0 to readCount - 1 do
                        if (buffer[i] = '\n' && builder.Length > 0 && builder[builder.Length - 1] = '\r') then
                            builder.Remove(builder.Length - 1, 1) |> ignore
                            let line = builder.ToString()
                            printfn "%d >> %s" clientId line
                            builder.Clear() |> ignore
                            match processor line with
                            | None -> ()
                            | Some command ->                                
                                let response = handleResponse command
                                let line = response |> serializeValue
                                printfn "%d << %s" clientId line
                                do! writeToClient cl line
                        else
                            builder.Append(buffer[i]) |> ignore
            with _ -> let client = clientTaskList |> List.find (fun conn -> conn.Id = clientId)
                      let { Stream = stream; CancellationTokenSource = cts; WorkerTask = task }= client
                      cts.Cancel()
                      task.Dispose()
                      stream.Dispose()
                      clientTaskList <- clientTaskList |> removeFirst (fun conn -> conn.Id = clientId)
                      printfn "%s disconnected" endpoint
        }
    listenWorkflow

let listen port = 
    let mutable id = 0
    let listenWorkflow =  
        task { 
            let listener = new TcpListener(IPAddress.Any, port)
            listener.Start()
            printfn "Start listening on port %i" port
            while true do
                let! client = listener.AcceptTcpClientAsync() |> Async.AwaitTask
                let endpoint = (client.Client.RemoteEndPoint :?> IPEndPoint).Address.ToString()
                id <- id + 1
                printfn "Client connected: %A, id: %d" endpoint id
                let cts = new CancellationTokenSource();
                let clientListenTask = Task.Run (fun () -> listenForMessages id endpoint (client.GetStream()), cts.Token)
                addClientTask { Id = id; CancellationTokenSource = cts; WorkerTask = clientListenTask; Stream = client.GetStream() }
        }
    listenWorkflow
