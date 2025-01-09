    type 'a t = {
        queue: 'a Queue.t;
        empty: Condition.t;
        nonempty: Condition.t;
        mutex: Mutex.t;
    }
    let of_seq x = 
        {
            queue = (Queue.of_seq x);
            empty = Condition.create();
            nonempty = Condition.create();
            mutex = Mutex.create();
        }
    let of_list x = 
        of_seq (List.to_seq x)

    let enqueue q a = 
        Mutex.protect q.mutex (fun _ -> 
                if q.queue |> Queue.length = 0 then 
                    Condition.broadcast q.nonempty
                else 
                    ();
                Queue.add a q.queue 
            )
    let enqueue_list q a = 
        if a |> List.is_empty then 
            ()
        else
            Mutex.protect q.mutex (fun _ -> 
                    if q.queue |> Queue.length = 0 then 
                        Condition.broadcast q.nonempty
                    else 
                        ();
                    List.iter (fun i -> Queue.add i q.queue) a
                )
    let try_dequeue q = 
        Mutex.protect q.mutex (fun _ -> 
            if q.queue |> Queue.length = 0 then 
                None 
            else 
                let broadcast = q.queue |> Queue.length = 1 in
                let res = Queue.pop q.queue in
                if broadcast then 
                    Condition.broadcast q.empty
                else 
                    ();
                Some(res)
        )
    let dequeue q = 
        Mutex.protect q.mutex (fun _ -> 
            while q.queue |> Queue.is_empty do 
                Condition.wait q.nonempty q.mutex
            done;
            let broadcast = q.queue |> Queue.length = 1 in
            let res = Queue.pop q.queue in
            if broadcast then 
                Condition.broadcast q.empty
            else 
                ();
            res
        )
