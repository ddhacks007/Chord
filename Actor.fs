module Actor
open Akka.Actor
open Akka.Configuration
open System
open Types
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent
open System.Collections

        

type Actor(id: int, m: int, init: bool, is_state_2: bool) =

    inherit UntypedActor()
    let mutable sv: IActorRef = Unchecked.defaultof<IActorRef> // Initialize to null
    let mutable pv: IActorRef = Unchecked.defaultof<IActorRef> 
    let fts = int (Math.Log(float (m)) / Math.Log(2.0)) + 1
    let mutable ft : (int * IActorRef) [][] = [||]
    let mutable s_id: int = id
    let mutable p_id: int = id
    let mutable init_state: bool = init
    let mutable second_state: bool = is_state_2
    
    member this.is_state_2
        with get() = second_state
        and set(value) = second_state <- value

    member this.id = id;
    member this.m = m
    
    member this.ft_size 
        with get() = fts

    member this.init 
        with get() = init_state
        and set(value) = init_state <- value

    member this.sid 
        with get() = s_id
        and set(value) = s_id <- value
    
    member this.pid 
        with get() = p_id 
        and set(value) = p_id <- value

    member this.successor 
        with get() = sv
        and set(value) = sv <- value

    member this.predecessor 
        with get() = pv
        and set(value) = pv <- value

    member this.fingerTable
        with get() = ft
        and set(value) = ft <- value

    member this.setUpPredAndSucessor(ref) =
        this.sid <- id
        this.pid <- id
        this.successor <- ref
        this.predecessor <- ref


    member this.InitializeFingerTable(ref) =
        this.fingerTable <- Array.init this.ft_size  (fun x -> [| ((id + (1 <<< x)) % m, ref) |])
        this.fingerTable.[this.ft_size-1].[0] <- (id, ref)

    member this.UpdateFingerTable(key: int, newRef: IActorRef) =
        for outerIndex in 0 .. (Array.length this.fingerTable - 1) do
                let (tableKey, _) = this.fingerTable.[outerIndex].[0]
                if tableKey = key then
                    this.fingerTable.[outerIndex].[0] <- (key, newRef)


    member this.DisplayTable(selfRef) =
        let mutable askForIdTasks = []  // List to store all the tasks
        let mutable idPairs = []  // List to store the pairs of intVal and associated ids

        this.fingerTable
        |> Array.iter (fun innerArr ->
            innerArr
            |> Array.iter (fun (intVal: int, nodeVal: IActorRef) ->
                if nodeVal = selfRef then
                    idPairs <- (intVal, this.id) :: idPairs
                else
                    let task = nodeVal.Ask<int>(GetID).ContinueWith(fun (t: Task<int>) ->
                        if t.Status = TaskStatus.RanToCompletion then
                            idPairs <- (intVal, t.Result) :: idPairs
                    )
                    askForIdTasks <- task :: askForIdTasks
            )
        )

        Task.WhenAll(askForIdTasks |> Array.ofList).ContinueWith(fun _ ->
            idPairs
            |> List.iter (fun (intVal, askForId) ->
                printfn "%d-----%d" intVal askForId
            )
            printfn "Sucessor is %d and Predecessor is %d " this.sid this.pid
        ) |> ignore



    override this.OnReceive(message: obj) =
        printfn "pass"