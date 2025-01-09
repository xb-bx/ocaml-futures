open Futures
let parse_get_req req = 
    let lines = String.split_on_char '\n' req in
    match lines with
    | [] -> None 
    | line :: _ -> 
        print_endline line;
        let first_line = String.split_on_char ' ' line in 
        print_endline"";
        List.iter (fun x ->  print_endline x) first_line;
        print_endline"";
        (match first_line with 
        | "GET" :: path :: _ -> 
            print_endline path;
            Some(path)
        | _ -> None)

let url_decode base_path str = 
    let str_list = List.of_seq (String.to_seq str) in
    let rec dec acc lst = 
        match lst with
        | [] -> acc
        | '%' :: first :: second :: rest -> 
            let i = int_of_string (String.of_seq (List.to_seq ['0'; 'x';first; second])) in
            let c = Char.chr i in
            dec (acc @ [c]) rest
        | c :: rest -> 
            dec (acc @ [c]) rest
    in match (dec [] str_list |> List.to_seq |> String.of_seq) with
    | "/" -> base_path
    | s -> Filename.concat base_path s |> Str.global_replace (Str.regexp "//+") "/"
    
type path_type = | Dir | File 
let check_path path = 
    let statcatch filik =
        try 
            let f = Unix.stat filik in
            Some f.st_kind
        with 
        | Unix.Unix_error _ -> None
    in
    let stat = statcatch path in 
    match stat with
    | Some(Unix.S_DIR) -> Some(Dir)
    | Some(Unix.S_REG) -> Some(File)
    | _ -> None
    
let send_404 socket =
    let msg = "HTTP/1.1 404 NOT FOUND\nContent-Length: 0\n\n" in 
    let^ _ = uring_write socket (Cstruct.of_string msg) in finished ()

let send_file socket path =
    let^ file = read_file path in
    match file with 
    | Ok(buf) -> 
        let len = (Cstruct.length buf) in 
        let^ _ = uring_write socket (Cstruct.of_string (Printf.sprintf "HTTP/1.1 200 OK\nContent-Type: text/html\nContent-Length: %i\n\n" len)) in
        let^ _ = uring_write socket buf in finished ()
    | _ -> 
        send_404 socket
let prepare_dir_message base_path dir files = 

    let b = match String.ends_with ~suffix:"/" dir with
    | true -> dir
    | false -> String.cat dir "/" in
    let base = String.sub b (String.length base_path) (String.length b - String.length base_path) in
    let rec prep acc files = 
        match files with
        | [] -> acc
        | file :: rest -> 
            prep (String.cat acc (Printf.sprintf "<a href=\"%s%s\">%s</a><br>" base file file)) rest
    in prep "" files
let send_dir base_path socket path =
    let files = Sys.readdir path |> Array.to_list in
    let html = prepare_dir_message base_path path files in
    let html' = Printf.sprintf "<!DOCTYPE html><html><head><title>%s</title><link rel=\"icon\" type=\"image/x-icon\" href=\"/favicon.ico\"></head><body>%s</body></html>" path html  in
    let len = String.length html' in
    let msg = Printf.sprintf "HTTP/1.1 200 OK\nContent-Type: text/html\nContent-Length: %i\n\n" len in
    let msg' = String.cat msg html' in
    let^ _ = uring_write socket (Cstruct.of_string msg') in finished ()
    
    

let client socket base_path = 
        let buf = Cstruct.create 4096 in
        let^ nread = uring_read socket buf in
        if nread = 0 then 
            finished ()
        else 
            let req = parse_get_req (Cstruct.to_string buf) in
            print_endline (Cstruct.to_string buf);
            match req with 
            | Some(path) -> 
                let path = (url_decode base_path path) in
                let^ _ = 
                    (match check_path path with 
                        | Some(Dir) -> send_dir base_path socket path
                        | Some(File) -> send_file socket path
                        | None -> send_404 socket) in

                Unix.close socket;
                finished ()
            | _ -> 
                let message = Printf.sprintf "HTTP/1.1 404 NOT FOUND\nContent-Length: 0\n\n" in
                let^ _ = uring_write socket (Cstruct.of_string message) in
                Unix.close socket;
                finished ()
        


let listen port base_path = 
    let socket = Unix.socket PF_INET SOCK_STREAM 0 in
    let addr = Unix.inet_addr_of_string "127.0.0.1" in 
    let sockadr = Unix.ADDR_INET(addr, port) in 
    Unix.setsockopt socket SO_REUSEADDR true;
    let _ = Unix.bind socket sockadr in
    let _ = Unix.listen socket 511 in 
    let clientaddr = Uring.Sockaddr.create() in
    Printf.printf "Listening on %i\n" port;
    flush_all();
    let^ _ = future_forever_loop (fun _-> let^ c = uring_accept socket clientaddr in enqueue_fut (client (Obj.magic c) base_path)) in
    finished()
let () = 
    let (port, base_path) = match Array.to_list Sys.argv with 
    | _ :: port :: base_path :: _ when int_of_string_opt port |> Option.is_some -> (int_of_string port, base_path)
    | _ :: port :: _ when int_of_string_opt port |> Option.is_some -> (int_of_string port, ".")
    | _ -> 6969, "." in
    let q = Cqueue.of_list [listen port base_path] in
    let ctx = { queued_futures= q; futures = 
        ref [
        ]; io_sleeping = ref []; time_sleeping = ref[]; uring = Uring.create ~queue_depth: 9999  ~polling_timeout: 0 (); lastJobId = ref 0; mutex = Mutex.create();} in
    let ctx1 = { queued_futures= q; futures = 
        ref [
        ]; io_sleeping = ref []; time_sleeping = ref[]; uring = Uring.create ~queue_depth: 9999  ~polling_timeout: 0 (); lastJobId = ref 0; mutex = Mutex.create();} in
    let _ = Thread.create worker ctx1 in
    worker ctx;
