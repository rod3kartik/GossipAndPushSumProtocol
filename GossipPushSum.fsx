#r "bin/Debug/netcoreapp3.1/Akka.dll"
#r "bin/Debug/netcoreapp3.1/Akka.FSharp.dll"
#time "on"

open System
open Akka
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

let system = System.create "system" (Configuration.defaultConfig())
let args = fsi.CommandLineArgs

let nActors = (int) args.[1]
let topology = (args.[2])
let algorithm = (args.[3])


type command1 = 
    | Start0 of int
    | Gossip0 of String
    | Dummy0 of String
    | CompletedMsg0 of String

type command2 = 
    | Start1 of int*Map<int,IActorRef>*int
    | Gossip1 of String
    | Dummy1 of String
    | CompletedMsg1 of String

type command3 = 
    | Start2 of int
    | Gossip2 of String
    | Dummy2 of String
    | CompletedMsg2 of String


type command4 = 
    | Gossip3 of decimal*decimal*Set<int>
    | Dummy3 of string
    | CompletedMsg3 of string
    | Start3 of int

type command5 = 
    | Gossip4 of decimal*decimal*Set<int>*int
    | CompletedMsg4 of string
    | Start4 of int*Map<int,IActorRef>

type command6 = 
    | Start5 of int*int*Map<int*int,IActorRef>*int*int
    | Gossip5 of decimal*decimal*Set<IActorRef>
    | CompletedMsg5 of string

if algorithm = "gossip" then
////////////////////////////////////////////  FUll   ////////////////////////
    if topology = "full" then 
        let limit_number = 10
        let mutable indexMap = Map.empty

        let mutable childRefs = []
        let mutable completedActors = 0

        let mutable countActor = 0;
        let mutable processActor = null
        let rand = new Random()   
        let mutable globalMap = Map.empty      
        let n = nActors
            
        let manager (mailbox: Actor<_>)=
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with 
                | CompletedMsg0 msg ->
                    completedActors <- completedActors + 1
                
                return! loop()
            }
            loop()
        let manager_ref= spawn system "manager" manager
            
        let child  (mailbox : Actor<_>) =
            let mutable mV = -1
            let mutable counter = 0
            let mutable counter1 = 0
            let mutable completedflag = false
        
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                | Start0 index ->
                    mV <- index
                | Gossip0 msg ->
                    counter <-counter + 1
                   
                    mailbox.Self <! Dummy0 "msg"
                        //system.Stop(mailbox.Self)
                | Dummy0 msg ->
                    counter1 <- counter1 + 1
                    let random = new Random()
                    if(counter < limit_number || counter1%10=0) then
                        
                        let mutable num =mV
                        while (num =mV ) do
                            num <- random.Next(0,nActors-1)
                        childRefs.[num] <! Gossip0 "some message"
                        
                    else
                        //printf "else"
                        if completedflag = false then
                            //sizes.[mV] <- true
                            
                            manager_ref <! CompletedMsg0 "some message"
                            completedflag <-true
                        
                    mailbox.Self <! Dummy0 "msg"
                        //system.Stop(mailbox.Self)
                return! loop()        
            }
            loop()            
           
        childRefs <- [for i in 1..nActors do yield (spawn system (sprintf "actor%i" i) child)]

        for i in 0..nActors-1 do 
            childRefs.[i] <! Start0 i

        for i in 0..nActors-1 do
            indexMap <- indexMap.Add(childRefs.[i],i)

        let stopWatch = System.Diagnostics.Stopwatch.StartNew()

        childRefs.[0] <! Gossip0 "some message" 
        let mutable flag = false
        while not flag do
            if completedActors >= nActors-2 then
                flag <- true
                stopWatch.Stop()

        //printfn "%A" countMap
        printf "total time in Seconds:%A\n"(stopWatch.Elapsed.TotalSeconds)
        

////////////////////////////////////////////  line   ////////////////////////


    elif topology = "line" then
        let mutable globalActorsMap = Map.empty
        let mutable completedActors = 0    
        let n = nActors
        let random = new Random()
        let manager  (mailbox : Actor<_>) =
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                | CompletedMsg1 msg->
                    completedActors <- completedActors + 1
                    printfn "Actor %i completed" completedActors
                return! loop()
            }
            loop()

        let manager_ref= spawn system "manager" manager
        
        let myActor  (mailbox : Actor<_>) =
            let mutable Gossipcount =0
            let mutable neighboursCount = 0
            let mutable temp_flag = true
            let mutable actor_not_completed = true
            let mutable DummyCount = 0
            let mutable onlyOnce = true
            let mutable myValue = -1
            let mutable myNeighbours = Map.empty
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                |Start1 (value,globalMap,totalActor)->
                    neighboursCount <- totalActor
                    myValue <- value
                    if(myValue = 1) then
                        myNeighbours <- myNeighbours.Add(0,globalMap.[2])
                    elif myValue = totalActor then
                        myNeighbours <- myNeighbours.Add(0,globalMap.[totalActor-1])
                    else
                        myNeighbours <- myNeighbours.Add(0,globalMap.[myValue - 1]).Add(1,globalMap.[myValue + 1])
                    neighboursCount <- myNeighbours.Count

                |Gossip1 (msg) ->
                    Gossipcount <- Gossipcount+1
                    if onlyOnce then
                        onlyOnce <- false
                        mailbox.Self <! Dummy1 "Start spreading the rumor"

                |Dummy1 (msg)->
                    DummyCount <- DummyCount + 1
                    if DummyCount%100 = 0 then
                        let num = random.Next(0,neighboursCount)
                        myNeighbours.[num] <! Gossip1 "This is a rumor"
                    if(temp_flag && Gossipcount>10) then
                        temp_flag <- false
                        actor_not_completed <- false
                        manager_ref <! CompletedMsg1 "done"
                        
                    if actor_not_completed then
                        let num = random.Next(0,neighboursCount)
                        myNeighbours.[num] <! Gossip1 "This is a rumor"
                    
                  
                    mailbox.Self <! Dummy1 "Spread rumor again"
                return! loop()
            }
            loop()
            
        for i = 1 to n do
            globalActorsMap <- globalActorsMap.Add(i,spawn system (sprintf "actor%i" i) myActor)

        for i =1 to n do
            let tempMap = globalActorsMap
            globalActorsMap.[i] <! Start1 (i,tempMap,n)

        let mutable rand = 0
        rand <- random.Next(0,n-1)
        globalActorsMap.[rand] <! Gossip1 "This is a rumor"
        let stopWatch = System.Diagnostics.Stopwatch.StartNew()
        let mutable temp = true
        while temp do
            if completedActors>=n-n/20 then 
                temp <-false
                stopWatch.Stop()

        printf "total time in Seconds:%A\n"(stopWatch.Elapsed.TotalSeconds)

////////////////////////////////////////////  2D   ////////////////////////

    elif topology = "2D" then
        let temp = sqrt(nActors|>double)|>int
        let mutable completedActors = 0 
        //let n = (Math.Sqrt(numberOfActor|>float))|>int
        let random = new Random()
        let limit_number  = 10
        let mutable globalMap = Map.empty      
        let manager  (mailbox : Actor<_>) =
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                | CompletedMsg2 msg->
                    completedActors <- completedActors + 1
                    //printfn "%i" countActor
                return! loop()
            }
            loop()

        let manager_ref= spawn system "manager" manager
        let refs = spawn system "one" manager
        let childRefs = Array2D.create temp temp refs 
        let mutable indexMap : Map<IActorRef, (int)array> =Map.empty

        
        let child  (mailbox : Actor<_>) =
            let mutable mV = -1
            let mutable counter = 0
            let mutable counter1 = 0
            let mutable completedflag = false
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with 
                | Start2 index ->
                    mV <- index
                | Gossip2 msg ->  
                    counter <- counter + 1
                        
                    mailbox.Self <! Dummy2 "msg"
                  
                |Dummy2 msg -> 
                    counter1 <- counter1 + 1
                    if(counter < limit_number || counter1%10 = 0) then
                        if((indexMap.[mailbox.Self]).[0] = 0 && (indexMap.[mailbox.Self]).[1] = 0) then
                            let neighours = Array.create 2 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1] + 1]
                            let mutable rand = -1
                            rand <- random.Next(0,2)
                            neighours.[rand] <! Gossip2 "some message"

                        elif((indexMap.[mailbox.Self]).[0] = 0 && (indexMap.[mailbox.Self]).[1] = temp-1) then
                            let neighours = Array.create 2 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            let mutable rand = -1
                            rand <- random.Next(0,2)
                            neighours.[rand] <! Gossip2 "some message"
                        elif((indexMap.[mailbox.Self]).[0] = temp-1 && (indexMap.[mailbox.Self]).[1] = 0) then 
                            let neighours = Array.create 2 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            let mutable rand = -1
                            rand <- random.Next(0,2)
                            neighours.[rand] <! Gossip2 "some message"
                        elif((indexMap.[mailbox.Self]).[0] = temp-1 && (indexMap.[mailbox.Self]).[1] = temp-1) then
                            let neighours = Array.create 2 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            let mutable rand = -1
                            rand <- random.Next(0,2)
                            neighours.[rand] <! Gossip2 "some message"
                        elif((indexMap.[mailbox.Self]).[0] = 0) then
                            let neighours = Array.create 3 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            let mutable rand = -1
                            rand <- random.Next(0,3)
                            neighours.[rand] <! Gossip2 "some message"
                        elif((indexMap.[mailbox.Self]).[0] = temp-1) then
                            let neighours = Array.create 3 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            let mutable rand = -1
                            rand <- random.Next(0,3)
                            neighours.[rand] <! Gossip2 "some message"
                        elif((indexMap.[mailbox.Self]).[1] = 0) then
                            let neighours = Array.create 3 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            let mutable rand = -1
                            rand <- random.Next(0,3)
                            neighours.[rand] <! Gossip2 "some message"
                        elif((indexMap.[mailbox.Self]).[1] = temp-1) then
                            let neighours = Array.create 3 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            let mutable rand = -1
                            rand <- random.Next(0,3)
                            neighours.[rand] <! Gossip2 "some message"
                        else
                            let neighours = Array.create 4 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[3]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            let mutable rand = -1
                            rand <- random.Next(0,4)
                            neighours.[rand] <! Gossip2 "some message"
                        //mailbox.Self <! Dummy "msg"
                    else
                        if completedflag = false then
                 
                            manager_ref <! CompletedMsg2 "some message"
                            completedflag <-true
                    mailbox.Self <! Dummy2 "msg"
                return! loop()        
            }
            loop()
                
        for i in 1..temp do
            for j in 1..temp do 
        
                childRefs.[i-1,j-1] <- spawn system (sprintf "actor%i_%i" i j) child
       

        for i in 0..temp-1 do
            for j in 0.. temp-1 do 
                indexMap <-indexMap.Add(childRefs.[i,j],[|i;j|])

        let stopWatch = System.Diagnostics.Stopwatch.StartNew()


        childRefs.[random.Next(0,temp-1),random.Next(0,temp-1)] <! Gossip2 "some message"


        let mutable flag = false
        while not flag do
            //let mutable c = 0
            if completedActors >= indexMap.Count then
                flag <- true
                stopWatch.Stop()

        printf "total time in Seconds:%A\n"(stopWatch.Elapsed.TotalSeconds)

        

////////////////////////////////////////////  imp2D   ////////////////////////
    elif topology = "imp2D" then 
        let random = new System.Random()
        let nActors = 10
        let temp = sqrt(nActors|>double)|>int
        let limit_number = 10
        let mutable countMap = Map.empty
        
        
        //let mutable childRefs = []
        //let mutable flag_array = []
        let mutable completedActors = 0
        

        let manager (mailbox : Actor<_>) =
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                | CompletedMsg2 msg ->
                    completedActors <- completedActors + 1
                    printfn "Actor %i completed" completedActors
            
                return! loop()
            }
            loop()
        
        let refs= spawn system "one" manager
        let manager_ref = spawn system "manager" manager
        let childRefs = Array2D.create temp temp refs 
        let mutable indexMap : Map<IActorRef, (int)array> =Map.empty
        let mutable totalIndex: Map<int,IActorRef> = Map.empty 
        
        let child (mailbox: Actor<_>)=
            let mutable mV = -1
            let mutable counter = 0
            let mutable counter1 = 0
            let mutable completedflag = false
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                //| Start index ->
                //    mV <- index
                | Gossip2 msg -> 
                    counter <- counter + 1
                    mailbox.Self <! Dummy2 "msg"
                    
                | Dummy2 msg ->
                    counter1 <- counter1 + 1
                    if(counter < limit_number || counter1%10 = 0) then
                    //if(countMap.[mailbox.Self] < limit_number) then
                        //countMap <-countMap.Add(mailbox.Self,countMap.[mailbox.Self]+1)
                        //printf "here"
                        if((indexMap.[mailbox.Self]).[0] = 0 && (indexMap.[mailbox.Self]).[1] = 0) then
                            let neighours = Array.create 3 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1] + 1]

                            let mutable randomNeighour = 0
                            while(mailbox.Self = totalIndex.[randomNeighour] && neighours.[0]=totalIndex.[randomNeighour] && neighours.[1]=totalIndex.[randomNeighour]) do
                                randomNeighour <- random.Next(0,totalIndex.Count)
                            neighours.[2]<- totalIndex.[randomNeighour]
                            
                            let mutable rand = 0
                            rand <- random.Next(0,3)
                            neighours.[rand] <! Gossip2 "some message"

                        elif((indexMap.[mailbox.Self]).[0] = 0 && (indexMap.[mailbox.Self]).[1] = temp-1) then
                            let neighours = Array.create 3 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            let mutable randomNeighour = 0
                            while(mailbox.Self = totalIndex.[randomNeighour] && neighours.[0]=totalIndex.[randomNeighour] && neighours.[1]=totalIndex.[randomNeighour]) do
                                randomNeighour <- random.Next(0,totalIndex.Count)
                            neighours.[2]<- totalIndex.[randomNeighour]
                            
                            let mutable rand = 0
                            rand <- random.Next(0,3)
                            neighours.[rand] <! Gossip2 "some message"

                        elif((indexMap.[mailbox.Self]).[0] = temp-1 && (indexMap.[mailbox.Self]).[1] = 0) then 
                            let neighours = Array.create 3 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            let mutable randomNeighour = 0
                            while(mailbox.Self = totalIndex.[randomNeighour] && neighours.[0]=totalIndex.[randomNeighour] && neighours.[1]=totalIndex.[randomNeighour]) do
                                randomNeighour <- random.Next(0,totalIndex.Count)
                            neighours.[2]<- totalIndex.[randomNeighour]
                            let mutable rand = 0
                            rand <- random.Next(0,3)
                            neighours.[rand] <! Gossip2 "some message"

                        elif((indexMap.[mailbox.Self]).[0] = temp-1 && (indexMap.[mailbox.Self]).[1] = temp-1) then
                            let neighours = Array.create 3 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            let mutable randomNeighour = 0
                            while(mailbox.Self = totalIndex.[randomNeighour] && neighours.[0]=totalIndex.[randomNeighour] && neighours.[1]=totalIndex.[randomNeighour]) do
                                randomNeighour <- random.Next(0,totalIndex.Count)
                            neighours.[2]<- totalIndex.[randomNeighour]
                            
                            let mutable rand = 0
                            rand <- random.Next(0,3)
                            neighours.[rand] <! Gossip2 "some message"

                        elif((indexMap.[mailbox.Self]).[0] = 0) then
                            let neighours = Array.create 4 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            let mutable randomNeighour = 0
                            while(mailbox.Self = totalIndex.[randomNeighour] && neighours.[0]=totalIndex.[randomNeighour] && neighours.[1]=totalIndex.[randomNeighour] && neighours.[2]=totalIndex.[randomNeighour]) do
                                randomNeighour <- random.Next(0,totalIndex.Count)
                            neighours.[3]<- totalIndex.[randomNeighour]
                            
                            let mutable rand = 0
                            rand <- random.Next(0,4)
                            neighours.[rand] <! Gossip2 "some message"

                        elif((indexMap.[mailbox.Self]).[0] = temp-1) then
                            let neighours = Array.create 4 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            let mutable randomNeighour = 0
                            while(mailbox.Self = totalIndex.[randomNeighour] && neighours.[0]=totalIndex.[randomNeighour] && neighours.[1]=totalIndex.[randomNeighour] && neighours.[2]=totalIndex.[randomNeighour]) do
                                randomNeighour <- random.Next(0,totalIndex.Count)
                            neighours.[3]<- totalIndex.[randomNeighour]
                            
                            let mutable rand = 0
                            rand <- random.Next(0,4)
                            neighours.[rand] <! Gossip2 "some message"

                        elif((indexMap.[mailbox.Self]).[1] = 0) then
                            let neighours = Array.create 4 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            let mutable randomNeighour = 0
                            while(mailbox.Self = totalIndex.[randomNeighour] && neighours.[0]=totalIndex.[randomNeighour] && neighours.[1]=totalIndex.[randomNeighour] && neighours.[2]=totalIndex.[randomNeighour]) do
                                randomNeighour <- random.Next(0,totalIndex.Count)
                            neighours.[3]<- totalIndex.[randomNeighour]
                            let mutable rand = 0
                            rand <- random.Next(0,4)

                            neighours.[rand] <! Gossip2 "some message"


                        elif((indexMap.[mailbox.Self]).[1] = temp-1) then
                            let neighours = Array.create 4 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            let mutable randomNeighour = 0
                            while(mailbox.Self = totalIndex.[randomNeighour] && neighours.[0]=totalIndex.[randomNeighour] && neighours.[1]=totalIndex.[randomNeighour] && neighours.[2]=totalIndex.[randomNeighour]) do
                                randomNeighour <- random.Next(0,totalIndex.Count)
                            neighours.[3]<- totalIndex.[randomNeighour]
                            let mutable rand = 0
                            rand <- random.Next(0,4)
                            neighours.[rand] <! Gossip2 "some message"

                        else
                            let neighours = Array.create 5 refs
                            neighours.[0]<- childRefs.[(indexMap.[mailbox.Self]).[0]-1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[1]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]-1]
                            neighours.[2]<- childRefs.[(indexMap.[mailbox.Self]).[0]+1,(indexMap.[mailbox.Self]).[1]]
                            neighours.[3]<- childRefs.[(indexMap.[mailbox.Self]).[0],(indexMap.[mailbox.Self]).[1]+1]
                            let mutable randomNeighour = 0
                            while(mailbox.Self = totalIndex.[randomNeighour] && neighours.[0]=totalIndex.[randomNeighour] && neighours.[1]=totalIndex.[randomNeighour] && neighours.[2]=totalIndex.[randomNeighour] && neighours.[3]=totalIndex.[randomNeighour]) do
                                randomNeighour <- random.Next(0,totalIndex.Count)
                            neighours.[4]<- totalIndex.[randomNeighour]
                            
                            let mutable rand = 0
                            rand <- random.Next(0,5)
                            neighours.[rand] <! Gossip2 "some message"                
                    else
                        if completedflag = false then
                            manager_ref <! CompletedMsg2 "some message"
                            completedflag <-true
                    mailbox.Self <! Dummy2 "msg"
                return! loop()        
            }
            loop()
        let mutable totalIndexforMap = -1
        printf "I'm in"
        for i in 1..temp do
            for j in 1..temp do 
                //childRefs.[0,0] <- spawn system (sprintf "actor%i%i" i j) child
                totalIndexforMap <- totalIndexforMap + 1
                childRefs.[i-1,j-1] <- spawn system (sprintf "actor%i_%i" i j) child
                //childRefs.[i-1,j-1] <! Start totalIndexforMap 
                totalIndex <- totalIndex.Add(totalIndexforMap,childRefs.[i-1,j-1])
                

        //printf "%A" childRefs.[4,3]
        for i in 0..temp-1 do
            for j in 0.. temp-1 do 
                countMap <-countMap.Add(childRefs.[i,j],0)

        for i in 0..temp-1 do
            for j in 0.. temp-1 do 
                indexMap <-indexMap.Add(childRefs.[i,j],[|i;j|])

        //printf "%A" countMap
        let stopWatch = System.Diagnostics.Stopwatch.StartNew()

        childRefs.[random.Next(0,temp-1),random.Next(0,temp-1)] <! Gossip2 "some message"


        //printf "%A" sizes
        let mutable flag = false
        while not flag do
            //let mutable c = 0
            if completedActors >= indexMap.Count then
            
                flag <- true
                stopWatch.Stop()

        printf "total time in Seconds:%A\n"(stopWatch.Elapsed.TotalSeconds)


elif algorithm = "push-sum" then 

////////////////////////////////////////////  FUll  Pushsum ////////////////////////
    if topology = "full" then 
        let mutable converged_actors_set = Set.empty
        let mutable childRefs = []
        let mutable completedActors = 0
        let random = new Random()
        let manager (mailbox: Actor<_>)=
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with 
                | CompletedMsg3 msg ->
                    completedActors <- completedActors + 1
                    printfn "Actor %i completed" completedActors
                return! loop()
            }
            loop()
        let manager_ref= spawn system "manager" manager
        let child (mailbox: Actor<_>)=
            let mutable mV = -1
            let mutable s_value = 0|>decimal
            let mutable w_value = 0|>decimal
            let mutable ratio = 1|>decimal
            let mutable count =0
            let mutable actor_converged = false
            let mutable num =0
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                | Start3 index ->
                    mV <- index
                    num <-mV
                    s_value <-mV|>decimal
                    w_value <- 1|>decimal
                    ratio <- s_value/w_value
                    //mailbox.Self <! Gossip (mV,1)
                | Gossip3 (s,w,set) ->
                    //counter <-counter + 1
                    if actor_converged = false then
                        //printfn "gossip called"
                        s_value <- s_value + s
                        w_value <- w_value + w
                        //ratio <- s_value/w_value
                        //printfn "here"
                        let mutable num1 = -1
                        while (num1 = -1 ||set.Contains(num1)) do
                            num1 <- random.Next(0,nActors-1)
                        //printfn "num is %i" num1 
                        s_value <- s_value/(2|>decimal)
                        w_value <-w_value/(2|>decimal)
                        let mutable current = (s_value/w_value)|>decimal
                        if(Math.Abs(current - ratio) > (0.0000000001|>decimal)) then
                            ratio <- current
                            count <-0
                            childRefs.[num1] <! Gossip3 (s_value,w_value,set)
                        else
                            count <-count + 1
                            //printfn "count is %i" count
                            if count >= 3 then
                                actor_converged <-true
                                //converged_actor_array.[num] <-true
                                let temp = set.Add(num)
                                
                                while (temp.Contains(num1)) do
                                    num1 <- random.Next(0,nActors-1)
                                
                                manager_ref <! CompletedMsg3 "done"

                                childRefs.[num1] <! Gossip3 (0|>decimal,0|>decimal,temp)
                            else
                                childRefs.[num1] <! Gossip3 (s_value,w_value,set)
                                    
                return! loop()        
            }
            loop()


        childRefs <- [for i in 1..nActors do yield (spawn system (sprintf "actor%i" i) child)]

        for i in 0..nActors-1 do 
            //countMap <-countMap.Add(childRefs.[i],0)
            childRefs.[i] <! Start3 i

        let stopWatch = System.Diagnostics.Stopwatch.StartNew()

        childRefs.[random.Next(0,nActors-1)] <! Gossip3 (0|>decimal,0|>decimal,converged_actors_set)
        printf "iM in"
        let mutable c = 0
        
        let mutable flag = false

        while not flag do
            //let mutable c = 0
            if completedActors >= nActors-2 then
                flag <- true
                stopWatch.Stop()

        printf "total time in Seconds:%A\n"(stopWatch.Elapsed.TotalSeconds)
        

////////////////////////////////////////////  line PushSum  ////////////////////////

    elif topology = "line" then 
        //let mutable countActor = 0;
        //let mutable processActor = null
        let random = new Random()
        let mutable converged_actors_set = Set.empty.Add(0).Add(nActors+1)
        let mutable globalMap = Map.empty
        let mutable completedActors = 0
        
        let manager (mailbox: Actor<_>)=
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with 
                | CompletedMsg4 msg ->
            
                    completedActors <- completedActors + 1
                    printfn "completed actors %A" completedActors
                return! loop()
            }
            loop()
        let manager_ref= spawn system "manager" manager

        let child (mailbox: Actor<_>)=
            let mutable mV = -1
            let mutable neighbourCount = 0
            let mutable mapOfAllNodes = Map.empty
            let mutable myNeighoursSet = Set.empty
            let mutable s_value = 0|>decimal
            let mutable w_value = 0|>decimal
            let mutable ratio = 1|>decimal
            let mutable not_converged = true
            let mutable count =0
            let mutable check_once = true
            let mutable num =0
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                | Start4 (index,globalMap) ->
                    mV <- index
                    num <-mV
                    s_value <-mV|>decimal
                    w_value <- 1|>decimal
                    mapOfAllNodes <- globalMap
                    neighbourCount <- mapOfAllNodes.Count
                    ratio <- s_value/w_value
                    
                    if(mV=1) then 
                        myNeighoursSet <- myNeighoursSet.Add(2)
                    
                    elif (mV = globalMap.Count) then 
                        myNeighoursSet <- myNeighoursSet.Add(globalMap.Count-1)
                    
                    else
                        myNeighoursSet <- myNeighoursSet.Add(mV+1).Add(mV-1)
                    //mailbox.Self <! Gossip (mV,1)
                | Gossip4 (s1,w1,set1,counter) ->
                    let set = set1
                    s_value <- s_value + s1
                    w_value <- w_value + w1
                    let mutable current = (s_value/w_value)|>decimal
                    if(Math.Abs(current - ratio) > (0.0000000001|>decimal)) then
                        ratio <- current
                        count <-0
                    else
                        count <-count + 1
                    if (count>=3) then
                        if check_once then
                            manager_ref <! CompletedMsg4 "done"
                            not_converged <- false
                            check_once <- false
                            let temp = set.Add(num)
                            let mutable num = mV
                            if (myNeighoursSet.IsSubsetOf(temp)) then 
                                while (temp.Contains(num)) do 
                                    num <- random.Next(1,neighbourCount+1)
                            else 
                                while (temp.Contains(num)) do 
                                    num <- random.Next(mV-1,mV+2)
                            s_value <- s_value/(2|>decimal)
                            w_value <- w_value/(2|>decimal)
                            mapOfAllNodes.[num] <! Gossip4(s_value,w_value,temp,counter+1)
                    if not_converged then
                        let mutable num = mV
                        //let mutable num = myValue
                        if(set.Count = neighbourCount+1 && not (set.Contains(num))) then 
                            mapOfAllNodes.[num] <! Gossip4 (0|>decimal,0|>decimal,set,counter+1)
                        else
                            if (myNeighoursSet.IsSubsetOf(set)) then 
                                while (set.Contains(num) || num = mV) do 
                                    num <- random.Next(1,neighbourCount+1)
                            else 
                                while (set.Contains(num) || num = mV) do 
                                    num <- random.Next(mV-1,mV+2)
                            s_value <- s_value/(2|>decimal)
                            w_value <- w_value/(2|>decimal)
                            mapOfAllNodes.[num] <! Gossip4 (s_value,w_value,set,counter+1)
                                     
                return! loop()        
            }
            loop()


        for i = 1 to nActors do
            globalMap <- globalMap.Add(i,spawn system (sprintf "actor%i" i) child)
            
        for i in 1..nActors do 
            //countMap <-countMap.Add(childRefs.[i],0)
            let tempMap = globalMap
            globalMap.[i] <! Start4 (i,tempMap)
        let stopWatch = System.Diagnostics.Stopwatch.StartNew()

        globalMap.[random.Next(0,nActors-1)] <! Gossip4 (0|>decimal,0|>decimal,converged_actors_set,0)

        let mutable flag = false
        while not flag do
            //let mutable c = 0
            if completedActors >= nActors-2 then
                flag <- true
                stopWatch.Stop()

        printf "total time in Seconds:%A\n"(stopWatch.Elapsed.TotalSeconds)


////////////////////////////////////////////  2D PushSum  ////////////////////////
    
    elif topology = "2D" then
        let mutable completedActors = 0;
        let rand = new Random()
        let mutable globalMap = Map.empty
        let n = (Math.Sqrt(nActors|>float))|>int
        
        let manager (mailbox: Actor<_>)=
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with 
                | CompletedMsg5 msg ->
                    completedActors <- completedActors + 1
                    printfn "Actor %i completed" completedActors
                return! loop()
            }
            loop()
        
        let manager_ref= spawn system "manager" manager
        
        let myActor  (mailbox : Actor<_>) =
            let random = new Random()
            let mutable count =0
            let mutable flag = true
            let mutable Once = true
            let mutable neighbour = Map.empty
            let mutable s_value = -1.0 |> decimal
            let mutable w_value = 1.0 |> decimal
            let mutable ratio = 0.0 |> decimal
            //let change = 0.0000000001 |> decimal
            let direction_in_x = [-1;1;0;0]
            let direction_in_y = [0;0;+1;-1]
            let mutable xaxis = -1
            let mutable yaxis = -1
            let mutable neighbourCount = 0
            let mutable everyNeighour = Map.empty
            let mutable mySet = Set.empty<IActorRef>
            //let two = 2 |> decimal
            
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                |Start5 (x,y,mapofEachActor,n,local_s_value)->
                    xaxis <- x
                    yaxis <- y
                    for i = 0 to direction_in_y.Length-1 do
                        let x = xaxis + direction_in_x.[i]
                        let y = yaxis + direction_in_y.[i]
                        if(x >= 0 && x < n && y >= 0 && y < n) then
                            neighbour <- neighbour.Add(neighbourCount,mapofEachActor.[(x,y)])
                            neighbourCount <- neighbourCount + 1
                            mySet <- mySet.Add(mapofEachActor.[(x,y)])
                    let mutable index = 0
                    for entry in mapofEachActor do
                        everyNeighour <- everyNeighour.Add(index,entry.Value)
                        index <-index+1
                    neighbourCount <- neighbour.Count
                    s_value <- (local_s_value)|>decimal
                    ratio <- s_value/w_value
        
                |Gossip5 (s1,w1,set) ->
                    s_value <- s_value + s1
                    w_value <- w_value + w1
                    let current = s_value/w_value
        
        
                    if (Math.Abs(current - ratio)) > (0.0000000001 |> decimal) then
                        ratio <- current
                        count <- 0
                    else 
                        count <- count + 1
                    
                    if(Once && count>=3) then
                        flag <- false
                        let Set_temp = set.Add(mailbox.Self)
                        Once <- false
                        s_value <- s_value/(2|>decimal)
                        w_value <- w_value/(2|>decimal)
                        if (mySet.IsSubsetOf(Set_temp)) then
                            let mutable num = random.Next(0,everyNeighour.Count)
                            let mutable actorRef = everyNeighour.[num]
                            while(Set_temp.Contains(actorRef)) do
                                 num <- random.Next(0,everyNeighour.Count)
                                 actorRef <- everyNeighour.[num]
                            actorRef <! Gossip5 (s_value,w_value,Set_temp)
                        else
                            let mutable num = random.Next(0,neighbourCount)
                            let mutable actorRef = neighbour.[num]
                            
                            while(Set_temp.Contains(actorRef)) do
                                num <- random.Next(0,neighbourCount)
                                actorRef <- neighbour.[num]
                            actorRef <!  Gossip5 (s_value,w_value,Set_temp)
                        manager_ref <! CompletedMsg5 "msg"
        
        
                    if flag then
                        s_value <- s_value/(2|>decimal)
                        w_value <- w_value/(2|>decimal)
                        if (mySet.IsSubsetOf(set)) then
                            let mutable actorRef = neighbour.[0]
                            let mutable num = random.Next(0,everyNeighour.Count)
                            while(set.Contains(actorRef)) do
                                 num <- random.Next(0,everyNeighour.Count)
                                 actorRef <- everyNeighour.[num]
                            actorRef <! Gossip5 (s_value,w_value,set)
                        else
                            let mutable num = random.Next(0,neighbourCount)
                            let mutable tempActor = neighbour.[num]
                            
                            while(set.Contains(tempActor)) do
                                num <- random.Next(0,neighbourCount)
                                tempActor <- neighbour.[num]
                            tempActor <!  Gossip5 (s_value,w_value,set)       
                return! loop()
            }
            loop()
        
        
        for i = 0 to n - 1 do
            for j = 0 to n - 1 do
                globalMap <- globalMap.Add((i,j),spawn system (sprintf "actor_%i_%i" i j) myActor)
        
        let mutable myNum = 1  
        for i = 0 to n - 1 do
            for j = 0 to n - 1 do
                let map = globalMap
                globalMap.[(i,j)] <! Start5 (i,j,map,n,myNum)
                myNum <- myNum + 1
        
        let converged = Set.empty<IActorRef>
        
        let stopwatch = System.Diagnostics.Stopwatch.StartNew()
        globalMap.[0,0] <! Gossip5 (0|>decimal,0|>decimal,converged)
        
        let mutable temp = true
        
        
        while temp do
            if completedActors>=(n*n)-1 then 
                temp <-false
        
        printfn "%i" completedActors
        stopwatch.Stop()
        printfn "%A" stopwatch.Elapsed.TotalMilliseconds

////////////////////////////////////////////  Imp2D PushSum  ////////////////////////

    elif topology = "imp2D" then
        let mutable completedActors = 0;
        let rand = new Random()
        let mutable globalMap = Map.empty
        let n = (Math.Sqrt(nActors|>float))|>int
        
        let manager (mailbox: Actor<_>)=
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with 
                | CompletedMsg5 msg ->
                    completedActors <- completedActors + 1
                    printfn "Actor %i completed" completedActors
                return! loop()
            }
            loop()
        
        let manager_ref= spawn system "manager" manager
                
        
        let child  (mailbox : Actor<_>) =
            let random = new Random()
            let mutable count = 0
            let mutable flag = true
            let mutable Once = true
            let mutable neighbour = Map.empty
            let mutable s_value = -1.0 |> decimal
            let mutable w_value = 1.0 |> decimal
            let mutable ratio = 0.0 |> decimal
           
            let direction_in_x = [-1;1;0;0]
            let direction_in_y = [0;0;+1;-1]
            let mutable xaxis = -1
            let mutable yaxis = -1
            let mutable countOfNeighours = 0
            let mutable everyneighour = Map.empty
            let mutable individual_set = Set.empty<IActorRef>
            //let two = 2 |> decimal
            
            let rec loop() = actor {
                let! msg = mailbox.Receive()
                match msg with
                |Start5 (x,y,mapOfEachActor,n,local_s_value)->
                    xaxis <- x
                    yaxis <- y
                    for i = 0 to direction_in_y.Length-1 do
                        let x = xaxis + direction_in_x.[i]
                        let y = yaxis + direction_in_y.[i]
                        if(x >= 0 && x < n && y >= 0 && y < n) then
                            neighbour <- neighbour.Add(countOfNeighours,mapOfEachActor.[(x,y)])
                            countOfNeighours <- countOfNeighours + 1
                            individual_set <- individual_set.Add(mapOfEachActor.[(x,y)])
                    let mutable index = 0
                    for entry in mapOfEachActor do
                        everyneighour <- everyneighour.Add(index,entry.Value)
                        index <-index+1
                    countOfNeighours <- neighbour.Count
                    s_value <- (local_s_value)|>decimal
                    ratio <- s_value/w_value
        
                |Gossip5 (s1,w1,set1) ->
                    s_value <- s_value + s1
                    w_value <- w_value + w1
                    let current = s_value/w_value
                    let mutable set = set1
        
                    if set1.Count = everyneighour.Count-1 then
                        manager_ref <! CompletedMsg5 "msg"
                        set <- set.Add(mailbox.Self)
        
                    if (Math.Abs(current - ratio)) > (0.0000000001 |> decimal) then
                        ratio <- current
                        count <- 0
                    else 
                        count <- count + 1
                    
                    if(Once && count>=3) then
                        flag <- false
                        let tempSet = set.Add(mailbox.Self)
                        Once <- false
                        s_value <- s_value/(2 |> decimal)
                        w_value <- w_value/(2 |> decimal)
                        let mutable tempActorRef = null
                        if (individual_set.IsSubsetOf(tempSet)) then
                            let mutable num = random.Next(0,everyneighour.Count)
                            tempActorRef <- everyneighour.[num]
                            while(tempSet.Contains(tempActorRef)) do
                                 num <- random.Next(0,everyneighour.Count)
                                 tempActorRef <- everyneighour.[num]
                            tempActorRef <! Gossip5 (s_value,w_value,tempSet)
                        else
                            let mutable num = random.Next(0,countOfNeighours+1)
                            if num = countOfNeighours then 
                                let mutable num1 = random.Next(0,everyneighour.Count)
                                tempActorRef <- everyneighour.[num1]
                                while(tempSet.Contains(tempActorRef)) do
                                     num1 <- random.Next(0,everyneighour.Count)
                                     tempActorRef <- everyneighour.[num1]
                            else
                                tempActorRef <- neighbour.[num]                 
                                while(tempSet.Contains(tempActorRef)) do
                                    num <- random.Next(0,countOfNeighours)
                                    tempActorRef <- neighbour.[num]
                            tempActorRef <!  Gossip5 (s_value,w_value,tempSet)
                        manager_ref <! CompletedMsg5 "msg"
                    if flag then
                        s_value <- s_value/(2 |> decimal)
                        w_value <- w_value/(2 |> decimal)
        
                        let mutable tempActorRef = null
                        if (individual_set.IsSubsetOf(set)) then
                            let mutable num = random.Next(0,everyneighour.Count)
                            tempActorRef <- everyneighour.[num]
                            while(set.Contains(tempActorRef)) do
                                 num <- random.Next(0,everyneighour.Count)
                                 tempActorRef <- everyneighour.[num]
                            tempActorRef <! Gossip5 (s_value,w_value,set)
                        else
                            let mutable num = random.Next(0,countOfNeighours+1)
                            if num = countOfNeighours then 
                                let mutable num1 = random.Next(0,everyneighour.Count)
                                tempActorRef <- everyneighour.[num1]
                                while(set.Contains(tempActorRef)) do
                                     num1 <- random.Next(0,everyneighour.Count)
                                     tempActorRef <- everyneighour.[num1]
                            else
                                tempActorRef <- neighbour.[num]                 
                                while(set.Contains(tempActorRef)) do
                                    num <- random.Next(0,countOfNeighours)
                                    tempActorRef <- neighbour.[num]
                            tempActorRef <!  Gossip5 (s_value,w_value,set)
        
                return! loop()
            }
            loop()
        for i = 0 to n - 1 do
            for j = 0 to n - 1 do
                globalMap <- globalMap.Add((i,j),spawn system (sprintf "actor_%i_%i" i j) child)
        
        let mutable myNum = 1  
        for i = 0 to n - 1 do
            for j = 0 to n - 1 do
                let map = globalMap
                globalMap.[(i,j)] <! Start5 (i,j,map,n,myNum)
                myNum <- myNum + 1
        
        let converged = Set.empty<IActorRef>
        let stopwatch = System.Diagnostics.Stopwatch.StartNew()
        globalMap.[0,0] <! Gossip5 (0|>decimal,0|>decimal,converged)
        
        let mutable temp = true 
        while temp do
            if completedActors>=(n*n) then 
                temp <-false
        
        printfn "%i" completedActors
        stopwatch.Stop()
        printfn "%A" stopwatch.Elapsed.TotalMilliseconds

    else 
        printfn "Please input correct algorithm!"

else 
    printfn "Please input correct protocol"
        


        
        







