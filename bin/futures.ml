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

type ioReqType = 
| Read of Cstruct.t * Optint.Int63.t
| Write of Cstruct.t * Optint.Int63.t
| Accept of Uring.Sockaddr.t
type ioReq = { fd: file_descr; id: int ref; typ: ioReqType; result: int ref; }

type sleeper = 
| IOSleep of ioReq
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
and executionContext = { futures: (unit, unit) future list ref; queued_futures: (unit, unit) future Cqueue.t; io_sleeping: ((unit, unit) future * ioReq) list ref; time_sleeping: ((unit, unit) future * (Core.Time_ns.t * int)) list ref; uring: int Uring.t; lastJobId: int ref; mutex: Mutex.t; } 
let enqueue_future ctx fut = 
    Cqueue.enqueue ctx.queued_futures fut
    
let next_id ctx = 
    let lastJob = ctx.lastJobId in
    lastJob := !lastJob + 1;
    !lastJob

let ioaccept id fd addr res = 
    Sleeping(IOSleep({ id = id; fd = fd; typ = Accept(addr); result = res}))
let iowrite id fd buf off res = 
    Sleeping(IOSleep({ id = id; fd = fd; typ = Write(buf, off); result = res}))
let ioread id fd buf off res = 
    Sleeping(IOSleep({ id = id; fd = fd; typ = Read(buf, off); result = res}))

    

let delay x = 
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
let future_map (trans: 'b -> 'c) (fut: ('a, 'b) future): ('a, 'c) future =
    let futt input ctx =
        match fut input ctx with
        | Sleeping a -> Sleeping a
        | Pending -> Pending
        | Finished res -> 
            let b = trans res in
            Finished b
    in futt
        
let future_bind (fut: ('a, 'b) future) (f: 'b -> ('a, 'c) future): ('a, 'c) future = 
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



let (let^) f a= 
    let fut = (future_bind (f) (a)) in fut

        
let future_do f: ('a, 'b) future = 
    let fut (x) (c: executionContext) = 
        f (c);
        Finished x
    in fut
let future_forever_loop (bodyf) = 
    let state = ref false in
    let body = ref (bodyf ()) in
    let fut input ctx = 
        match (!body) input ctx with
        | Finished _ -> 
            state := false;
            body := bodyf ();
            Pending
        | Sleeping s -> Sleeping s
        | Pending -> Pending
    in fut
let future_loop (cond) (bodyf) = 
    let state = ref false in
    let body = ref (bodyf ()) in
    let fut input ctx = 
        match (!state) with 
        | false -> 
            if cond() then
                let _ =
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

let sleeper_time sleepr = 
    match sleepr with 
    | TimeSleep (st, ms) -> (st,ms)
    | _ -> raise (Match_failure ("", 0,0))
let sleeper_uring sleepr = 
    match sleepr with 
    | IOSleep(r) -> r
    | _ -> raise (Match_failure ("", 0,0))
let all_sleeping_uring = 
    List.for_all (fun x -> match x with | IOSleep _  -> true | _ -> false)
let all_sleeping_time = 
    List.for_all (fun x -> match x with | TimeSleep _ -> true | _ -> false)
let get_end start ms = 
    let endd = Core.Time_ns.add start (Core.Time_ns.Span.of_int_ms ms) in
    endd


let find_minimal_timeout (sleepers: ((unit, unit) future * (Core.Time_ns.t * int)) list) = 
    let rec min cur lst = 
        match lst with 
        | (_, (start, endd)) :: tail -> 
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
    | IOSleep _ -> true
    | _ -> false

type 'a fuck_ocaml = { result: int; data: 'a } 
let wait t u = 
    match Uring.get_cqe_nonblocking u with 
    | Some r ->
        Some({result= r.result; data = r.data })
    | None -> 
        (match Uring.wait ~timeout:t u with 
        | Some r -> 
                Some({result= r.result; data = r.data })
        | None -> None) 
    
let enqueue_fut f = 
    let fut input ctx = 
        enqueue_future ctx (f |> future_map (fun _ -> ()));
        Finished input
    in fut

let get_ctx = 
    let fut _ ctx = 
        Finished ctx
    in fut
let uring_read fd buf = 
    let started = ref false in 
    let jobid = ref 0 in 
    let read = ref 0 in
    let fut _ _ = 
        match !started with
        | false -> 
            started := true;
            ioread jobid fd buf (Optint.Int63.of_int 0) read
        | true -> 
            Finished !read
    in fut
let uring_write fd buf = 
    let started = ref false in 
    let wrote = ref 0 in
    let jobid = ref 0 in 
    let fut _ _ = 
        match !started with
        | false -> 
            started := true;
            iowrite jobid fd buf (Optint.Int63.of_int 0) wrote
        | true -> 
            Finished !wrote
    in fut

            
let uring_accept socket addr = 
    let started = ref false in
    let ressocket = ref 0 in
    let jobid = ref 0 in
    let fut _ _ = 
        match !started with  
        | false -> 
            started := true;
            ioaccept jobid socket addr ressocket
        | true -> 
            Finished !(ressocket)
    in fut
let read_file file = 
    let opencatch filik =
        try 
            let f = Unix.openfile filik [Unix.O_RDONLY] 0 in
            let i = (Obj.magic f) in
            assert (i >= 0);
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
        Unix.close desc;
        finished (Ok(buf))
    | Error(e) -> finished (Error e)
    

let submit_ioreq ctx ioreq = 
    let uring = ctx.uring in 
    ioreq.id := next_id ctx;
    match ioreq.typ with 
    | Read(buf, off) -> 
        Uring.read uring ~file_offset:off ioreq.fd buf !(ioreq.id)
    | Write(buf, off) -> 
        Uring.write uring ~file_offset:off ioreq.fd buf !(ioreq.id)
    | Accept addr -> 
        Uring.accept uring ioreq.fd addr !(ioreq.id)

let worker ctx = 
    let futs = ctx.futures in
    while true do 
        let io_sleeping = !(ctx.io_sleeping) in 
        let time_sleeping = !(ctx.time_sleeping) in 
        if !futs |> List.length = 0 && (io_sleeping |> List.length) = 0 && (time_sleeping |> List.length) == 0 then 
            let s = Cqueue.dequeue ctx.queued_futures in
            futs :=  s :: !futs;
        else if !futs |> List.length = 0 then
            (match Cqueue.try_dequeue ctx.queued_futures with
            | Some(s) -> 
                futs :=  s :: !futs;
            | None -> ())
        ;
        let futures = !(ctx.futures) in 
        let next_sleeping_io = ref [] in
        let next_sleeping_time = ref time_sleeping in
        let next_futures = ref [] in
        let finished_id = ref None; in
        let wait_for_uring: int Uring.t -> int fuck_ocaml option = if (List.length futures) = 0 && (List.length io_sleeping) <> 0 && (List.length time_sleeping) = 0 then 
            wait (10.0)
        else if io_sleeping |> List.length = 0 then
            (fun _ -> None)
        else 
            let timeout = find_minimal_timeout  time_sleeping in
            wait timeout
        in
        next_sleeping_io := io_sleeping;
        let c = ref true in
        while !c do 
            c:= false;
            (match wait_for_uring ctx.uring with 
            | Some({result = r; data=id}) -> 
                let sleept = !next_sleeping_io |> List.find (fun ((_, i)) -> !((i).id) = id)  in
                let result_ref = (snd sleept).result in  
                result_ref := r;
                next_futures := (fst sleept) :: !next_futures; 
                finished_id := Some id
            | _ -> c := false;
            );
            (match !finished_id with 
            | None -> c := false;
            | Some(id) ->
                next_sleeping_io := !next_sleeping_io |> List.filter (fun ((_, sid)) -> !((sid).id) <> id)
            );
        done;
        for i = 0 to (time_sleeping |> List.length) - 1 do
            let (fut, (start, ms)) = List.nth time_sleeping i in
            let now = Core.Time_ns.now() in
            let endd = get_end start ms in 
            let dif = Core.Time_ns.Span.to_sec (Core.Time_ns.diff endd now) in
            if dif <= 0.0 then
                (next_futures := fut :: !next_futures; 
                next_sleeping_time := !(next_sleeping_time) |> List.filter (fun ((_, t)) -> let (st, st_ms) = (t) in st <> start || st_ms <> ms))
            else 
                ()
        done;
        for i = 0 to (List.length futures) - 1 do 
            let fut = List.nth futures i in
            let res = fut () ctx in
            match res with 
            | Finished _ -> 
                ()
            | Pending  -> 
                next_futures := fut :: !next_futures; 
            | Sleeping (IOSleep r) -> 
                submit_ioreq ctx r |> ignore;
                next_sleeping_io := (fut, (r)) :: !next_sleeping_io; 
                ()
            | Sleeping (TimeSleep (start, ms)) -> 
                next_sleeping_time := (fut, (start, ms)) :: !next_sleeping_time ;
            ;
        done;
        let _ = Uring.submit ctx.uring in
        let io_slps = ctx.io_sleeping in
        let time_slps = ctx.time_sleeping in
        futs := !next_futures;

        io_slps := !next_sleeping_io;
        time_slps := !next_sleeping_time;

        if (List.length futures) = 0 && (List.length !next_sleeping_io) = 0 && (List.length !next_sleeping_time) > 0 then 
            let timeout = find_minimal_timeout !next_sleeping_time in
            sleepf timeout
        else 
            ();
            ;
    done 
