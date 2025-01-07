[@@@warnerror "-unused-value-declaration"]
[@@@warnerror "-unused-var"]
[@@@warnerror "-unused-var-strict"]
[@@@warnerror "-unused-open"]
[@@@warnerror "-unused-constructor"]
[@@@warnerror "-unused-type-declaration"]
[@@@warnerror "-unused-field"]
[@@@warnerror "-nonreturning-statement"]
[@@@warnerror "-partial-match"]

open Unix

type sleeper = 
| UringSleep of int * int ref
| TimeSleep of Core.Time_ns.t * int
let uring_unwrap (uring: 'a Uring.completion_option) = 
    match uring with 
    | None -> 
        raise (Match_failure ("", 0,0))
    | Some({result; data}) -> result, data
type 'a futureResult =
| Pending
| Finished of 'a
| Sleeping of sleeper
let ppfutres f = 
    match f with 
    | Pending -> print_endline "pending"
    | Finished _ -> print_endline "finished" 
    | Sleeping _ -> print_endline "sleeping"
type ('a, 'b) future = 'a -> executionContext -> 'b futureResult
and executionContext = { futures: (unit, unit) future list ref; queued_futures: (unit, unit) future list ref; sleeping: ((unit, unit) future * sleeper) list ref; uring: int Uring.t; lastJobId: int ref } 
let enqueue_future ctx fut = 
    let futs = ctx.queued_futures in
    futs := fut :: !futs
    
let nextId ctx = 
    let lastJob = ctx.lastJobId in
    lastJob := !lastJob + 1;
    !lastJob



let readFileFut file = 
    let descr = Unix.openfile file [Unix.O_RDONLY] 0 in
    let stat = Unix.fstat descr in  
    let started = ref false in 
    let buf = Cstruct.create stat.st_size in
    let fut _ ctx = 
        match !started with 
        | false -> 
            let jobId = (nextId ctx) in
            let read = Uring.read ctx.uring descr buf jobId ~file_offset: (Optint.Int63.of_int 0) in
            (match read with 
            | Some(job) ->
                started := true;
                let _ = Uring.submit ctx.uring in 
                Sleeping (UringSleep (jobId, (ref 0)))
            | _ ->
                (*print_endline "zhopa";*)
                Finished (Bytes.create 0))
        | true -> 
            (*let (res, _) = ctx.uring |> Uring.get_cqe_nonblocking |> uring_unwrap in *)
            Unix.close descr;
            Finished(Cstruct.to_bytes buf)
        (*let res = Unix.read descr !byts 0 stat.st_size in *)
        (*match res with*)
        (*| r when (r = stat.st_size) -> Finished r*)
        (*| r ->*)
            (*print_string "read res";*)
            (*print_int r;*)
            (*print_endline ";";*)
            (*Sleeping descr*)
    in fut
    

let delay x = 
    (*let start = ref () in*)
    let started = ref false in 
    let fut _ _ =
        if ((!started) = false) then
            let _ = 0 in
            started := true;
            Sleeping (TimeSleep (Core.Time_ns.now(), x))
        else
            Finished x
    in fut

        
let finished a: ('a, 'b) future = 
    let fut _ _ =
        Finished a
    in fut
let futureMap (trans: 'b -> 'c) (fut: ('a, 'b) future): ('a, 'c) future =
    let futt input ctx =
        match fut input ctx with
        | Sleeping a -> Sleeping a
        | Pending -> Pending
        | Finished res -> 
            let b = trans res in
            Finished b
    in futt
        
let futureThenTupple (right: ('b, 'c) future) (left: ('a, 'b) future): ('a, ('b * 'c)) future = 
    let state: 'b futureResult option ref = ref None in
    let fut input ctx =
        match !state with 
        | None | Some Pending -> 
            let res = left input ctx in
            let ress = 
                match res with 
                | Sleeping a -> Sleeping a
                | Finished a -> 
                    state := Some (Finished a); 
                    Pending
                | Pending -> 
                    Pending
            in ress
        | Some (Sleeping s) -> Sleeping s 
        | Some (Finished a) -> 
            futureMap (fun aa -> a,aa) (right) a ctx
    in fut
let futureBind (fut: ('a, 'b) future) (f: 'b -> ('a, 'c) future): ('a, 'c) future = 
    let state = ref None in
    let ff input ctx = 
        match !state with 
        | None -> 
            (match fut input ctx with 
            | Pending -> Pending 
            | Sleeping s -> Sleeping s
            | Finished x -> 
                state := Some(f x);
                Pending
            )
        | Some(x) -> 
            x input ctx
    in ff

let futureThen (right: ('b, 'c) future) (left: ('a, 'b) future): ('a, 'c) future = 
    let state: 'b futureResult option ref = ref None in
    let fut input ctx =
        match !state with 
        | None | Some Pending -> 
            let res = left input ctx in
            let ress = 
                match res with 
                | Finished a -> 
                    state := Some (Finished a); 
                    Pending
                | Sleeping s -> Sleeping s
                | Pending -> 
                    Pending
            in ress
        | Some (Sleeping s) -> Sleeping s 
        | Some (Finished a) -> 
            right a ctx 
    in fut

let counter x = 
    let i = ref 0 in
    let fut (_) = 
        match (!i) < x with
        | true ->
            i := (!i) + 1;
            Pending
        | false ->
            Finished !i 
    in fut


let (let*) = Result.bind

let (and&) a f: ('a, 'b) future= 
    let fut = (futureThenTupple (f) (a)) in
    let futi input (ctx: executionContext) =
        match fut input ctx with 
        | Finished x -> Finished x
        | Sleeping s -> Sleeping s
        | Pending -> Pending
    in futi
let (let^) f a= 
    let fut = (futureBind (f) (a)) in fut
let (let&) (a: ('a, 'b) future) (f: ('b, 'c) future): ('a, 'c) future= 
    let fut = (futureThen (f) (a)) in
    let futi input (ctx: executionContext) =
        match fut input ctx with 
        | Finished x -> Finished x
        | Sleeping s -> Sleeping s
        | Pending -> Pending
    in futi


let return x = 
    let fut ctx = 
        Finished x
    in fut


        
        
let futureDo f: ('a, 'b) future = 
    let fut (x) (c: executionContext) = 
        f (c);
        Finished x
    in fut
let isNotFinished x = 
    match x with 
    | Finished _ -> false
    | Sleeping _ -> true
    | Pending -> true
let futureLoop (cond) (bodyf) = 
    let state = ref false in
    let body = ref (bodyf ()) in
    let fut input ctx = 
        (*!state |> string_of_bool |> print_endline ;*)
        match (!state) with 
        | false -> 
            if cond() then
                let a =
                    state := true
                in
                    Pending
            else 
                Finished ()
        | true -> 
            match (!body) input ctx with
            | Finished _ -> 
                state := false;
                body := bodyf ();
                Pending
            | Sleeping s -> Sleeping s
            | Pending -> Pending
    in fut
(*let exampleFut3 = *)
    (*let x = ref 100 in*)
    (*let& _ = futureLoop (fun () -> !x < 200000) (fun () -> ((futureDo (fun _ -> !x |> string_of_int |> print_endline ; x := !x + 1)) |> futureThen (delay 0)))*)
    (*in return x*)
(*let exampleFut4 = *)
    (*let x = ref 200000 in*)
    (*let& _ = futureLoop (fun () -> !x < 300000) (fun () -> ((futureDo (fun _ -> !x |> string_of_int |> print_endline ; x := !x + 1)) |> futureThen (delay 0)))*)
    (*in return x*)
(*let exampleFut2 = futureThen (futureDo (fun _ -> print_string "hello")) *)
(*let exampleFut3 = (futureThen (delay 5) (futureDo (fun _ -> print_endline "hello")))*)
let futIsNotFinished ctx input f = 
    ctx |> f input |> isNotFinished
    (*let zopha = futures |> List.filter (fun x -> match x () with | Finished _ -> false | Pending -> true) in*)
    (*()*)
    

let readText file =
    let& bytes = readFileFut file in 
    (Bytes.to_string bytes) |> print_endline;
    return ()
let sleeper_time sleepr = 
    match sleepr with 
    | TimeSleep (st, ms) -> (st,ms)
    | _ -> raise (Match_failure ("", 0,0))
let sleeper_uring sleepr = 
    match sleepr with 
    | UringSleep (id, refres) -> (id,refres)
    | _ -> raise (Match_failure ("", 0,0))
let all_sleeping_uring = 
    List.for_all (fun x -> match x with | UringSleep _ -> true | _ -> false)
let all_sleeping_time = 
    List.for_all (fun x -> match x with | TimeSleep _ -> true | _ -> false)
let get_end start ms = 
    let endd = Core.Time_ns.add start (Core.Time_ns.Span.of_int_ms ms) in
    endd

let find_minimal_timeout sleepers = 
    let rec min cur lst = 
        match lst with 
        | UringSleep _ :: tail -> 
            min cur tail
        | TimeSleep (start, endd) :: tail -> 
            let now = Core.Time_ns.now() in
            let endd = Core.Time_ns.add start (Core.Time_ns.Span.of_int_ms endd) in
            let dif = Core.Time_ns.Span.to_int_ms (Core.Time_ns.diff endd now) in
            if dif > 0 then
                min (Some (Core.Time_ns.Span.to_sec (Core.Time_ns.Span.of_int_ms dif))) tail
            else if dif = 0 then 
                0.0
            else
                0.0
        | [] -> 
            Option.value cur ~default: (0.0)
    in 
    let r = min None sleepers in
    r
let is_uring x = 
    match x with
    | UringSleep _ -> true
    | _ -> false

type 'a fuck_ocaml = { result: int; data: 'a } 
let wait t u = 
    match Uring.get_cqe_nonblocking u with 
    | Some r ->
        flush_all();
        Some({result= r.result; data = r.data })
    | None -> 
        (match Uring.wait ~timeout:t u with 
        | Some r -> 
        flush_all();
                Some({result= r.result; data = r.data })
        | None -> None) 
    
(*let () = *)
    (*listen 6969*)

let enqueue_fut f = 
    let fut input ctx = 
        enqueue_future ctx (f |> futureMap (fun _ -> ()));
        Finished input
    in fut
let enqueed_delay = enqueue_fut (delay 5)

let get_ctx = 
    let fut input ctx = 
        Finished ctx
    in fut
let uring_read fd buf = 
    let started = ref false in 
    let read = ref 0 in
    let fut _ ctx = 
        let jobid = nextId ctx in 
        match !started with
        | false -> 
            let res = Uring.read ctx.uring ~file_offset: (Optint.Int63.of_int 0) fd buf jobid in
            (match res with 
            | Some(j) -> 
                started := true;
                (*let _ = Uring.submit ctx.uring in *)
                Sleeping(UringSleep(jobid, read))
            | _ ->
                raise (Match_failure ("", 0,0)))
        | true -> 
            Finished !read
    in fut
let uring_write fd buf = 
    let started = ref false in 
    let wrote = ref 0 in
    let fut _ ctx = 
        let jobid = nextId ctx in 
        match !started with
        | false -> 
            let res = Uring.write ctx.uring ~file_offset: (Optint.Int63.of_int 0) fd buf jobid in
            (match res with 
            | Some(j) -> 
                started := true;
                (*let _ = Uring.submit ctx.uring in *)
                Sleeping(UringSleep(jobid, wrote))
            | _ ->
                raise (Match_failure ("", 0,0)))
        | true -> 
            Finished !wrote
    in fut

            
let uring_accept socket addr = 
    let started = ref false in
    let ressocket = ref 0 in
    let fut input ctx = 
        let jobid = nextId ctx in
        match !started with  
        | false -> 
                let acc = Uring.accept ctx.uring socket addr jobid in
                (match acc with 
                    | Some(j) -> 
                        let _ = Uring.submit ctx.uring in
                        started := true;
                        Sleeping(UringSleep (jobid, ressocket))
                    | _ -> 
                        print_endline "aboba";
                        raise (Match_failure ("", 0, 0)))
        | true -> 
            Finished !(ressocket)
    in fut
let readFile file = 
    let opencatch filik =
        try 
            let f = Unix.openfile filik [Unix.O_RDONLY] 0 in
            Unix.handle_unix_error (fun x -> Ok x) f
        with 
        | Unix.Unix_error (error, _, _) -> Error error
    in
    let descr = opencatch file in
    match descr with 
    | Ok(desc) -> 
        let stat = fstat desc in
        let buf = Cstruct.create stat.st_size in
        let^ _ = uring_read desc buf in
        finished (Ok(buf))
    | Error(e) -> finished (Error e)
    

let client socket = 
        let buf = Cstruct.create 4096 in
        let^ nread = uring_read socket buf in
        if nread = 0 then 
            finished ()
        else 
            let^ htmlres = readFile "t.html" in
            print_endline (Cstruct.to_string buf);
            match htmlres with 
            | Ok(html) -> 
                let length = Cstruct.length html in
                let message = Printf.sprintf "HTTP/1.1 200 OK\nContent-Type: text/html\nContent-Length: %i\n\n" length in
                let^ _ = uring_write socket (Cstruct.of_string message) in
                let^ _ = uring_write socket html in
                close socket;
                finished ()
            | _ -> 
                let message = Printf.sprintf "HTTP/1.1 404 NOT FOUND\nContent-Length: 0\n\n" in
                let^ _ = uring_write socket (Cstruct.of_string message) in
                close socket;
                finished ()
        


let listenFut port = 
    let socket = Unix.socket PF_INET SOCK_STREAM 0 in
    let addr = Unix.inet_addr_of_string "127.0.0.1" in 
    let sockadr = ADDR_INET(addr, port) in 
    Unix.setsockopt socket SO_REUSEADDR true;
    let _ = Unix.bind socket sockadr in
    let _ = Unix.listen socket 511 in 
    let clientaddr = Uring.Sockaddr.create() in
    let^ ctx = get_ctx in
    let^ a = futureLoop (fun _ -> true) (fun _-> let& ctx = get_ctx and& c = uring_accept socket clientaddr in enqueue_future ctx (client (Obj.magic c)); return()) in
    enqueue_future ctx (client (Obj.magic a));
    finished ()
let () = 
    let ctx = { queued_futures= ref []; futures = 
        ref [
            listenFut 3333;
        ]; sleeping = ref []; uring = Uring.create ~queue_depth: 10 ~polling_timeout: 0 (); lastJobId = ref 0} in
    while (List.length !(ctx.futures)) > 0|| (List.length !(ctx.sleeping)) > 0 do
        let now_time = Core.Time_ns.now() in
        let futures = !(ctx.futures) in 
        let sleeping = !(ctx.sleeping) in 
        let next_sleeping = ref [] in
        let next_futures = ref [] in
        let finished_id = ref None; in
        let wait_for_uring: int Uring.t -> int fuck_ocaml option = if (List.length futures) = 0 && all_sleeping_uring (List.map snd sleeping) then 
            wait (1.0)
        else if ((List.filter (fun x -> x |> snd |> is_uring) sleeping) |> List.length) = 0 then
            (fun _ -> None)
        else 
            let timeout = find_minimal_timeout (List.map snd sleeping) in
            wait timeout
        in
        (match wait_for_uring ctx.uring with 
        | Some({result = r; data=id}) -> 
            let sleept = sleeping |> List.filter (fun x -> x |> snd |> is_uring) |> List.find (fun ((_, i)) -> (fst (sleeper_uring i)) = id)  in
            let sleeper = sleept |> snd |> sleeper_uring |> snd in 
            sleeper := r;
            next_futures := (fst sleept) :: !next_futures; 
            finished_id := Some id
        | _ ->  ();
        );
        (match !finished_id with 
        | None -> next_sleeping := sleeping;
        | Some(id) ->
            next_sleeping := sleeping |> List.filter (fun ((_, sid)) -> if (is_uring sid) then (sid |> sleeper_uring |> fst) <> id else true)
        );
        let time_sleeping = List.filter (fun x -> x |> snd |> is_uring |> not) sleeping in
        for i = 0 to (time_sleeping |> List.length) - 1 do
            let (fut, TimeSleep(start, ms)) = List.nth time_sleeping i in
            let now = Core.Time_ns.now() in
            let endd = get_end start ms in 
            let dif = Core.Time_ns.Span.to_sec (Core.Time_ns.diff endd now) in
            if dif <= 0.0 then
                (next_futures := fut :: !next_futures; 
                next_sleeping := !(next_sleeping) |> List.filter (fun ((_, t)) -> if is_uring t then true else let (st, st_ms) = (sleeper_time t) in st <> start || st_ms <> ms))
            else 
                ()
            
        done;
        for i = 0 to (futures |> List.length) - 1 do 
            let fut = List.nth futures i in
            let res = fut () ctx in
            match res with 
            | Finished _ -> 
                ()
            | Pending  -> 
                next_futures := fut :: !next_futures; 
            | Sleeping (UringSleep (id, r)) -> 
                next_sleeping := (fut, UringSleep((id, r))) :: !next_sleeping; 
                ()
            | Sleeping (TimeSleep (start, ms)) -> 
                next_sleeping := (fut, TimeSleep(start, ms)) :: !next_sleeping ;
            ;
        done;
        let futs = ctx.futures in
        let slps = ctx.sleeping in
        let que = ctx.queued_futures in
        futs := List.append !(ctx.queued_futures) !next_futures;
        que := [];
        slps := !next_sleeping;

        if (List.length futures) = 0 &&all_sleeping_time (List.map snd sleeping) then 
            let timeout = find_minimal_timeout (List.map snd sleeping) in
            sleepf timeout
        else 
            ()
            ;
    done
    (*main ()*)
    

