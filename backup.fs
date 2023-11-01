let initActor (id: int) (m: int) (system: ActorSystem) (init: bool) (is_state_2: bool) = 
    let resultAsync = async {
        
        let actor = system.ActorOf(Props.Create(fun () -> new Node(id, m)), $"actor{id}")
        actor.Ask<_>(InitFingerTable) |> Async.AwaitTask
        if init=true then 
            actor.Ask<_>(AssignInit) |> Async.AwaitTask
        else 
            if is_state_2=true then 
                actor.Ask<_>(AssignState2) |> Async.AwaitTask
            else 
                printfn ""
        return actor 
    }
    resultAsync

let assignSuccessor (id: int) (actor: IActorRef) (base_: IActorRef) (base_id: int) (isFirstTwo: bool) = 
    let resultAsync = async {      
        if isFirstTwo then 
            actor.Ask<_>(UpdateSuccessor(base_, base_id)) |> Async.AwaitTask
        else 
            let (actorRefOpt, idOpt) = Async.RunSynchronously (findActorForKey id base_)
            match actorRefOpt, idOpt with
                | Some actorRef, Some id -> 
                    printfn "Received actorRef: %A with id %d" actorRef id
                    actor.Ask<_>(UpdateSuccessor(actorRef, id)) |> Async.AwaitTask
                | _ -> printfn "No actorRef or id received."
    }

let actor1 = Async.RunSynchronously (initActor id1 m system)
 
    let actor2 = Async.RunSynchronously (initActor id2 m system)
    assignSuccessor id2 actor2 actor1 1 true 
    assignSuccessor id1 actor1 actor2 2 true  


     match actorRefOpt, idOpt with
        | Some actorRef, Some id -> 
            printfn "Received actorRef: %A with id %d" actorRef id
            actorC.Tell(UpdateSuccessor(actorRef, id))
            Task.Delay(delayTime).Wait()
        | _ -> printfn "No actorRef or id received."
    Task.Delay(delayTime).Wait()


    actorB.Tell(AssignState2)
    Task.Delay(delayTime).Wait()


    actorC.Tell(InitFingerTable)
    Task.Delay(delayTime).Wait()

    actorD.Tell(InitFingerTable)
    Task.Delay(delayTime).Wait()

    actorE.Tell(InitFingerTable)
    Task.Delay(delayTime).Wait()

    
    // actorA.Tell(UpdateSuccessor(actorB, id2))
    // Task.Delay(delayTime).Wait()

    // actorA.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()

    // actorB.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()


    // Task.Delay(delayTime).Wait()

    // // actorB.Tell(Find(13))

    let (actorRefOpt, idOpt) = Async.RunSynchronously (findActorForKey id3 actorA)

    match actorRefOpt, idOpt with
        | Some actorRef, Some id -> 
            printfn "Received actorRef: %A with id %d" actorRef id
            actorC.Tell(UpdateSuccessor(actorRef, id))
            Task.Delay(delayTime).Wait()
        | _ -> printfn "No actorRef or id received."
    Task.Delay(delayTime).Wait()

    // actorC.Tell(DisplayTable)

    // actorA.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()
    // actorB.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()
   
    // actorD.Tell(DisplayTable)

    // let (actorRefOpt, idOpt) = Async.RunSynchronously (findActorForKey 5 actorB)
    // match actorRefOpt, idOpt with
    //     | Some actorRef, Some id -> 
    //         printfn "Received actorRef: %A with id %d" actorRef id

    // printfn "check from here .... "
    // let (actorRefOpt, idOpt) = Async.RunSynchronously (findActorForKey id3 actorA)

    let (actorRefOpt, idOpt) = Async.RunSynchronously (findActorForKey id4 actorA)
    match actorRefOpt, idOpt with
        | Some actorRef, Some id -> 
            printfn "Received actorRef: %A with id %d***********" actorRef id
            actorD.Tell(UpdateSuccessor(actorRef, id))
            Task.Delay(delayTime).Wait()
        | _ -> printfn "No actorRef or id received."
    
    Task.Delay(delayTime).Wait()


    let (actorRefOpt, idOpt) = Async.RunSynchronously (findActorForKey id5 actorB)
    match actorRefOpt, idOpt with
        | Some actorRef, Some id -> 
            printfn "Received actorRef: %A with id %d for 3" actorRef id
            actorE.Tell(UpdateSuccessor(actorRef, id))
            Task.Delay(delayTime).Wait()
        | _ -> printfn "No actorRef or id received."

    actorA.Tell(DisplayTable)
    Task.Delay(delayTime).Wait()

    actorB.Tell(DisplayTable)
    Task.Delay(delayTime).Wait()

    actorC.Tell(DisplayTable)
    Task.Delay(delayTime).Wait()
    
    actorD.Tell(DisplayTable)
    Task.Delay(delayTime).Wait()

    actorE.Tell(DisplayTable)
    Task.Delay(delayTime).Wait()

    // printfn "%A " actorA
    // actorA.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()
    // actorB.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()
    // actorC.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()
    // actorD.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()

    // match actorRefOption with
    // Some actorRef -> // Do something with actorRef
    // None          -> 

    // actorC.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()

   


    // actorC.Tell(DisplayTable)
    // Task.Delay(delayTime).Wait()



    // Task.Delay(delayTime).Wait()


    id3 <- 14
    let actorC = Async.RunSynchronously (initActor id3 m system false false)

    Async.RunSynchronously (assignSuccessor id3 actorC actorA id1 false)

    id3 <- 12
    let actorD = Async.RunSynchronously (initActor id3 m system false false)

    Async.RunSynchronously (assignSuccessor id3 actorD actorA id1 false)


    actorA.Tell(DisplayTable)