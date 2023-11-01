module Types
open Akka.Actor
open Akka.Configuration

type QueryMessage =
    | GetID
    | GetSuccessor
    | GetPredecessor
type Setters = 
    | InitFingerTable
type Show = 
    | DisplayTable

type Update = 
    | UpdateSuccessor of (IActorRef * int)
    | UpdatePredecessor of (IActorRef * int)
    | Find of (int * IActorRef * IActorRef * int)
    | NodeFound of (int * IActorRef * int * IActorRef* int )
    | AssignInit
    | Stabilize
    | AssignState2
    | FindKey of int