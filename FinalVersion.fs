open Akka.Actor
open Akka.Configuration
open System
open Actor
open Types
open System.Threading.Tasks
open Utils
open System.Collections.Generic
open System.Collections.Concurrent
open System.Collections

// Open your Actor module here
// open YourNamespace.Actor




type ConcurrentList<'T>() =
    let mutable list = ResizeArray<'T>()  // This is a mutable list
    let lockObj = obj()

    interface IEnumerable<'T> with
        member this.GetEnumerator () = 
            lock lockObj (fun () -> (list :> IEnumerable<'T>).GetEnumerator())

    interface IEnumerable with
        member this.GetEnumerator () = 
            lock lockObj (fun () -> (list :> IEnumerable).GetEnumerator()) :> IEnumerator

    member this.Add item =
        lock lockObj (fun () -> list.Add(item))

    member this.Remove item =
        lock lockObj (fun () -> list.Remove(item))

    member this.ToArray () =
        lock lockObj (fun () -> list.ToArray())

    member this.Contains(item: 'T) =
        lock lockObj (fun () -> list.Contains(item))
    
    member this.Clone() =
        lock lockObj (fun () -> 
            let newConcurrentList = ConcurrentList<'T>()
            list.ForEach(fun item -> newConcurrentList.Add(item))
            newConcurrentList
        )


type Node(id: int, m: int, init: bool, is_state_2: bool) = 
    inherit Actor(id, m, init, is_state_2)
    let mutable keyToRequestedActor = ConcurrentDictionary<int, ConcurrentList<IActorRef>>()
    let cache = new ConcurrentDictionary<int, IActorRef * int * int>()
    let mutable FTTracker = new System.Collections.Generic.List<int>()


    let findPrecedingNodeAsync (ft: (int * IActorRef) [][]) (key: int) (m: int) : Task<IActorRef> =
        let tcs = new TaskCompletionSource<IActorRef>()
        let mutable minDist = System.Int32.MaxValue
        let mutable closest: IActorRef option = None
        
        let dist key1 key2 =
            if key2 > key1 then key2 - key1
            else (m - key1) + key2

        let flatFt = Seq.concat ft

        let actorTaskMap: (IActorRef * Task<int>) [] = flatFt |> Seq.map (fun (_, actorRef) -> (actorRef, actorRef.Ask<int>(GetID))) |> Seq.toArray

        let askForIdTasks: Task<int> [] = actorTaskMap |> Array.map snd

        let allTasks = Task.WhenAll(askForIdTasks)
        allTasks.ContinueWith(fun (completedAllTask: Task) -> 
            match completedAllTask.Status with
            | TaskStatus.RanToCompletion ->
                for (actorRef, task) in actorTaskMap do
                    let actorId = task.Result
                    let d = dist actorId key
                    // printfn $"the {actorId} and the {key} has the distance {d} "
                    if d < minDist then
                        closest <- Some actorRef
                        minDist <- d
            | _ -> 
                printfn "Some tasks did not run to completion"

            tcs.SetResult(match closest with Some a -> a | None -> null :> IActorRef)
        ) |> ignore

        tcs.Task

    member this.UpdateSuccessor(successor: IActorRef, sid: int) =
        // printfn $"Calling Update Sucessor at {this.id} to {sid} "
        let self = this.Self
        let tasks = new List<Task<_>>() 
        let id = this.id
        let mutable wait: bool = false
        FTTracker = new System.Collections.Generic.List<int>()
        this.successor <- successor
        this.sid <- sid
        for index in 0 .. (Array.length this.fingerTable - 1) do
            let (key, node) = this.fingerTable.[index].[0]
            let rangeInSuccessor = inRange key this.id sid this.m  
            if key = id then 
                this.fingerTable.[index].[0] <- (key, self)
        
            else 
                if rangeInSuccessor then 
                    this.fingerTable.[index].[0] <- (key, successor)
                else 
                    if this.init = false then 
                        if this.is_state_2 = true then 
                            this.fingerTable.[index].[0] <- (key, self)
                        else 
                            // Use Ask and add the resulting Task to our list
                            wait  <- true
                            // printfn $"Adding key {key}"
                            FTTracker.Add(key) |> ignore

            
        for i in 0 .. (FTTracker.Count - 1) do
           
            // printfn $"requesting successor {sid} to find {FTTracker.[i]} from {id}"
            successor.Tell(Find(FTTracker.[i], self, self, 0))
            
                
       
        this.successor.Tell(UpdatePredecessor(this.Self, this.id))
        
                    

    // Helper functions
    member this.Stabilize() =
        let task = this.successor.Ask<(IActorRef * int)>(GetPredecessor)
        let self = this.Self
        task.ContinueWith(fun (completedTask: Task<IActorRef * int>) ->
            match completedTask.Status with
            | TaskStatus.RanToCompletion ->
                let result = completedTask.Result
                let (predRef, predId) = result
                if predRef <> self then 
                    self.Tell(UpdateSuccessor(predRef, predId)) 
                
            | TaskStatus.Faulted ->
                printfn "Task failed with exceptions: %A" completedTask.Exception.InnerExceptions
            | TaskStatus.Canceled ->
                printfn "Task was canceled"
            | _ ->
                printfn "Task did not complete successfully"
        ) |> ignore

    member private this.HandleQueryMessage(sender: IActorRef, query: QueryMessage) =
        match query with
        | GetID -> sender.Tell(this.id)
        | GetSuccessor -> sender.Tell((this.successor, this.sid))
        | GetPredecessor -> sender.Tell((this.predecessor, this.pid))
    
    member private this.HandleSetters(setter: Setters, selfRef: IActorRef) =
        match setter with
        | InitFingerTable -> 
            this.setUpPredAndSucessor(selfRef)
            this.InitializeFingerTable(selfRef)
        
    member private this.HandleDisplay(display: Show, selfRef: IActorRef) =
        match display with
        | DisplayTable -> this.DisplayTable(selfRef)
        
    // Main OnReceive function
    override this.OnReceive(message: obj) =
        let sender = this.Sender
        let self = this.Self
        let successor = this.successor
        // printfn "Received a message of type: %s" (message.GetType().ToString())
        
        match message with
            | :? QueryMessage as query -> this.HandleQueryMessage(sender, query)
            | :? Setters as setter -> this.HandleSetters(setter, this.Self)
            | :? Show as display -> this.HandleDisplay(display, this.Self)
            | :? Update as update -> 
                match update with
                    | AssignInit -> 
                        this.init <- true
                    | AssignState2 -> 
                        this.is_state_2 <- true

                    | UpdateSuccessor (successor, sid)-> 
                        this.UpdateSuccessor(successor, sid)
                        

                    | UpdatePredecessor (predecessor, pid) -> 
                        let oldPred = this.predecessor
                        let oldPid = this.pid
                        this.predecessor <- predecessor
                        this.pid <- pid
                        if oldPred <> this.Self then
                            // printfn "Called Stabilize to the old pid %d " oldPid
                            oldPred.Tell(Stabilize)
                        
                        
                        
                    | Stabilize  -> this.Stabilize()

                    | FindKey key -> 
                        let mutable valueOut: IActorRef * int * int = (null, 0, 0) // Initial value
                        let success = cache.TryGetValue(key, &valueOut)
                        if success then
                            let (Node, id, hops) = valueOut
                            this.Sender.Tell((Some(Node), id, hops))
                        else
                            this.Sender.Tell(((None: Option<IActorRef>), -1, -1))

                        
                    | NodeFound (key, Node, id, source, hops) -> 
                    //    printfn "Node found ... %d for key %d (received at %d)" id key this.id 
                        // printfn $"key: {key} found at node {this.id} {keyToRequestedActor.ContainsKey(key)} with hops: {hops-1}"
                        if source = this.Self then 
                            cache.TryAdd(key, (Node, id, hops)) |> ignore

                        this.UpdateFingerTable(key, Node) 

                        if keyToRequestedActor.ContainsKey(key) = true then 
                            let actorRefs = keyToRequestedActor.[key].Clone()
                            // printfn $"Cloned {key} to send it to the ref from {this.id}"
                            let itemsToRemove = ResizeArray<IActorRef>()

                            
                            for actorRef in actorRefs do
                                // printfn $"Sending {actorRef} about {key} from {this.id}"
                                actorRef.Tell(NodeFound(key, Node, id, source, hops))
                                
                                itemsToRemove.Add actorRef
                                
                            // Remove the items after enumeration is done
                            for item in itemsToRemove do
                                actorRefs.Remove(item) |> ignore
                        else 
                            () 
                            
                           
                    //    if FTTracker.Contains(key) then
                    //         FTTracker.Remove(key)
                    //         let size = FTTracker.Count
                    //         if size = 0 then 
                    //             printfn $"Updating the predecessor of  ${this.sid} with ${this.id}"
                    //             successor.Tell(UpdatePredecessor(this.Self, this.id))
                            

                    | Find (key, sender, source, hops) -> 
                        // printfn $"Trying to find {key} from {this.id}"

                        if (key = this.id) or (inRange key this.pid this.id this.m) then 
                            // printfn $"{key} is inrange for {this.id}"
                            sender.Tell(NodeFound(key, self, this.id, source, hops + 1))
                      
                        else
                            let precedingNodeTask = findPrecedingNodeAsync this.fingerTable key this.m
                            precedingNodeTask.ContinueWith(fun (completedTask: Task<IActorRef>) -> 
                                match completedTask.Status with
                                    | TaskStatus.RanToCompletion ->
                                        let mutable closestNode = completedTask.Result
                                        if obj.ReferenceEquals(closestNode, null) || closestNode = self || closestNode = sender   then
                                            closestNode <- this.successor
                                        
                                        if closestNode = self then 
                                            sender.Tell(NodeFound(key, self, this.id, source, hops + 1))
                                        else 

                                            closestNode.Ask<int>(GetID).ContinueWith(fun (completedTask: Task<int>) ->
                                                match completedTask.Status with
                                                | TaskStatus.RanToCompletion -> 
                                                    let closestNodeId = completedTask.Result
                                                    
                                                    // printfn "Queueing the %d reference inside Actor: %d waiting for the closest node %d" key this.id closestNodeId
                                                    if sender <> self then  
                                                        if keyToRequestedActor.ContainsKey(key) then
                                                            let existingList = keyToRequestedActor.[key]
                                                            if existingList.Contains(sender) = false then 
                                                                existingList.Add(sender)      
                                                            // else 
                                                            //     printfn $"The key {key} is already requested for {this.id}"
                                                        else
                                                            let newList = new ConcurrentList<IActorRef>()
                                                            newList.Add(sender)
                                                            // printfn $"Adding key {key} into the dict from {id}"
                                                            keyToRequestedActor.TryAdd(key, newList) |> ignore

                                                                                                    
                                                    closestNode.Tell(Find(key, self, source, hops + 1))
                                                
                                                | _ -> 
                                                    printfn "Task did not complete successfully"
                                            ) |> ignore

                                    | _ -> printfn "Got closest node has none"
                            ) |> ignore

            | _ -> printfn "Unknown message type"

let findActorForKey (key: int) (refActor: IActorRef) =
    let resultAsync = async {
        let mutable result: Option<IActorRef> = None
        let mutable id: Option<int> = None
        let mutable hops: Option<int> = None
        let mutable retry = 0
        refActor.Tell(Find(key, refActor, refActor, 0))

        while result.IsNone do
            if retry = 150 then 
                refActor.Tell(Find(key, refActor, refActor, 0))
                retry <- 0
            else 
                Task.Delay(100).Wait()
                let! tmpResult = refActor.Ask<Option<IActorRef> * int * int>(FindKey(key)) |> Async.AwaitTask
                let (actorOption, idOption, hopOption) = tmpResult
                // printfn $"trying to retrieve {key} {retry}"
                if actorOption.IsSome then
                    // printfn $"Got the result for {key} after {retry} "
                    result <- actorOption
                    id <- Some idOption  // Assume idOption is of type int
                    hops <- Some hopOption
                else 
                    retry <- retry + 1
        return (result, id, hops)
    }
    resultAsync


let initActor (id: int) (m: int) (system: ActorSystem) (init: bool) (is_state_2: bool) = 
    let resultAsync = async {
        
        let actor = system.ActorOf(Props.Create(fun () -> new Node(id, m, init, is_state_2)), $"actor{id}")
        actor.Ask<_>(InitFingerTable) |> Async.AwaitTask
        return actor
        
    }
    resultAsync

let assignSuccessor (id: int) (actor: IActorRef) (base_: IActorRef) (base_id: int) (isFirstTwo: bool) : Async<option<bool>> = 
    let currentId = id
    async {      
        let mutable result: option<bool> = None
        if isFirstTwo then 
            actor.Ask<_>(UpdateSuccessor(base_, base_id)) |> Async.AwaitTask
            return result
        else 
            let! (actorRefOpt, idOpt, hopCount) = findActorForKey id base_
            match actorRefOpt, idOpt, hopCount with
                | Some actorRef, Some id, Some hop -> 
                    // printfn "Received actorRef: %A with id %d with hops %d" actorRef id hop
                    actor.Tell(UpdateSuccessor(actorRef, id))

                    while result.IsNone do
                        do! Async.Sleep 100  // Replacing Task.Delay with Async.Sleep
                        let! (predecessor, pid) = actor.Ask<(IActorRef*int)>(GetPredecessor) |> Async.AwaitTask
                        // printfn $"{currentId}  predecessor is {pid}  "
                        if pid <> currentId then 
                            result <- Some true
                    return result
                | _ -> return result

    }


let computeTotalHopCount (actorKeyMap: List<IActorRef*int>) =
    let asyncTasks = actorKeyMap.ConvertAll(fun (actor, key) -> findActorForKey key actor)
    let mutable result = 0
    async {
        let! results = Async.Parallel asyncTasks
        let hopCounts = results |> Array.choose (fun (_, _, hops) -> hops)
        let total_hop_count = Array.sum hopCounts
        result <- total_hop_count
        return result
    }


let generateActorKeyMap (actors: List<IActorRef>) (m: int) (n: int) =
    let random = Random()
    let mutable actorKeyMap = List<(IActorRef*int)>()
    let mutable i = 0
    for actor in actors do
        for _ in [1..n] do
            let key = random.Next(0, m + 1)
            actorKeyMap.Add((actor, key))         
    actorKeyMap


[<EntryPoint>]
let main argv =
    // Setup actor system
    let startTime = DateTime.Now

    let config = ConfigurationFactory.ParseString("""akka { loglevel = WARNING }""")
    let system = ActorSystem.Create("MySystem", config)
    let random = new Random()
    let batch_count = 50
    // Create ActorA and ActorB
    let n = int argv.[0]
    let m = n + 1
    let num_nodes_to_find = int argv.[1]
    if m > n && m > 0 && n > 0 then 
        let id1 = 1
        let id2 = int (System.Math.Ceiling(float m/float 2))
        
        let uniqueIds = HashSet<int>()  
        let mutable id3 = 0
        uniqueIds.Add(id1)
        uniqueIds.Add(id2)
        
        let mutable delayTime = 200
        // let id5 = 
        let actors = new List<_>()  // To store the created actors

        let actorA = Async.RunSynchronously (initActor id1 m system true false)
        // let actorB = Async.RunSynchronously (initActor id2 m system false true)

        // Async.RunSynchronously (assignSuccessor id2 actorB actorA id1 true )

        // Async.RunSynchronously (assignSuccessor id1 actorA actorB id2 true )

        // actorA.Tell(DisplayTable)
        // Task.Delay(delayTime).Wait()
        // printfn $"{actorB}"
        // actorB.Tell(DisplayTable)
        let startTime = DateTime.Now

        actors.Add(actorA)

        let mutable id3 = 0
        let mutable list_of_actors = $"{id1},{id2},"
        for temp_id in 1..n-1 do
            if temp_id = 1 then 
                let actorA = actors.[0]
                let actorB = Async.RunSynchronously (initActor id2 m system false true)
                actors.Add(actorB)
                Async.RunSynchronously (assignSuccessor id2 actorB actorA id1 true )
                Async.RunSynchronously (assignSuccessor id1 actorA actorB id2 true )
                
            else 
                let mutable id3 = random.Next(0, m)
                while uniqueIds.Contains(id3) do
                    id3 <- random.Next(0, m)
                list_of_actors <- list_of_actors + $"{id3},"

                uniqueIds.Add(id3)
                // printfn $"*******************************************************................ Counter: {temp_id} Actor is created successfully with {id3} {list_of_actors} *********************************************************************"
                let actorN = Async.RunSynchronously (initActor id3 m system false false)
                actors.Add(actorN) 
                Async.RunSynchronously (assignSuccessor id3 actorN actorA id1 false)       

        
        let endTime = DateTime.Now
        let elapsedTime = endTime - startTime

        let endTime = DateTime.Now
        let elapsedTime = endTime - startTime
        let actorKeyMap = generateActorKeyMap actors m num_nodes_to_find
        let final_batch_count = if (actorKeyMap.Count%batch_count = 0) then batch_count else actorKeyMap.Count%batch_count
        let no_of_batches = int (System.Math.Ceiling(float actorKeyMap.Count/float batch_count))
        let batches = List.init no_of_batches (fun i -> actorKeyMap.GetRange(i*batch_count,  (if i=no_of_batches-1 then final_batch_count else batch_count ) ))
        let mutable totalHopTask = 0
        for i in 0..(batches.Length-1) do   
            // printfn $"batch {i}"          
            let tempTotalHop = Async.RunSynchronously (computeTotalHopCount (batches.[i]))
            totalHopTask <-  tempTotalHop + totalHopTask
        
        let average_hop_count = float totalHopTask/float ( num_nodes_to_find*actors.Count)
        
        printfn "%f" average_hop_count
    else 
        printfn "Kindly check your arguments the first-param is ring-size and the second-param is node size and both should be > 0"
        ()



    // system.Terminate().Wait()

    0 
