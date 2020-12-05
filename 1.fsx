#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
#r "nuget: MathNet.Numerics"

open System
open Akka
open Akka.FSharp
open Akka.Actor
open MathNet.Numerics

let system =
    System.create "system" (Configuration.defaultConfig ())

type Msg =
    | Initialize of int
    | InitializeServer of int
    | CreateClients of int
    | AssignFollowers of int
    | InitialTweet of int
    | Tweet of int * int
    | SendTweetToServer of int * string
    | SendTweetToClient of int * string
    | ReceiveTweetFromServer of int * string
    | TweetAck
    | InitialRetweet of int
    | Retweet of int * int
    | SendRetweetToServer of int * string
    | SendRetweetToClient of int * string
    | ReceiveRetweetFromServer of int * string
    | RetweetAck
    | InitialQueryAll
    | QueryAllRequest of int
    | QueryAll of int
    | QueryAllResponse of int * List<string>
    | InitialQueryHashtag
    | QueryHashtagRequest of int
    | QueryHashtag of int * string
    | QueryHashtagResponse of int * string * List<string>
    | InitialQueryMention
    | QueryMentionRequest of int
    | QueryMention of int * string
    | QueryMentionResponse of int * string * List<string>
    | CalcNumZero
    | InitialConnectDisconnect
    | Disconnect of int
    | Connect of int

let mutable numUsers = 100

let mutable simulatorRef = null
let mutable clientParentRef = null
let mutable serverRef = null

let mutable clientToNumFollowers: Map<int, int> = Map.empty
let mutable clientToClientRef: Map<int, IActorRef> = Map.empty
let mutable clientToFollowers: Map<int, Set<int>> = Map.empty
let mutable clientToFollowing: Map<int, Set<int>> = Map.empty

let mutable flag1 = false
let mutable temp = true

let mutable totalNumTweets = double 0
let mutable numTweets = double 0
let mutable numZeroFollowing = double 0
let mutable totalNumRetweets = double 0
let mutable numRetweets = double 0

let server (mailbox: Actor<_>) =

    let mutable clientToTweets: Map<int, List<string>> = Map.empty

    let rec loop () =
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | InitializeServer (numUsers) ->
                for i = 1 to numUsers do
                    clientToTweets <- clientToTweets.Add(i, List.empty)

            | SendTweetToServer (client, tweet) ->
                for follower in clientToFollowers.[client] do
                    let mutable allTweets = clientToTweets.[follower]
                    allTweets <- allTweets @ [ sprintf "Tweet: %s" tweet ]
                    clientToTweets <- clientToTweets.Add(follower, allTweets)
                    serverRef <! SendTweetToClient(follower, tweet)

            | SendTweetToClient (client, tweet) ->
                clientToClientRef.[client]
                <! ReceiveTweetFromServer(client, tweet)

            | TweetAck -> numTweets <- numTweets + double 1

            | SendRetweetToServer (client, retweet) ->
                for follower in clientToFollowers.[client] do
                    let mutable allTweets = clientToTweets.[follower]
                    allTweets <-
                        allTweets
                        @ [ sprintf "Retweet (by User%i): %s" client retweet ]
                    clientToTweets <- clientToTweets.Add(follower, allTweets)
                    serverRef
                    <! SendRetweetToClient(follower, retweet)

            | SendRetweetToClient (client, retweet) ->
                clientToClientRef.[client]
                <! ReceiveRetweetFromServer(client, retweet)

            | RetweetAck -> numRetweets <- numRetweets + double 1

            | QueryAll (client) ->
                clientToClientRef.[client]
                <! QueryAllResponse(client, clientToTweets.[client])

            | QueryHashtag (client, hashtag) ->
                let allTweets = clientToTweets.[client]
                let mutable response = List.empty
                for tweet in allTweets do
                    if tweet.IndexOf(hashtag) <> -1 then response <- response @ [ tweet ]
                clientToClientRef.[client]
                <! QueryHashtagResponse(client, hashtag, response)

            | QueryMention (client, mention) ->
                let allTweets = clientToTweets.[client]
                let mutable response = List.empty
                for tweet in allTweets do
                    if tweet.IndexOf(mention) <> -1 then response <- response @ [ tweet ]
                clientToClientRef.[client]
                <! QueryHashtagResponse(client, mention, response)

            | _ -> ()

            return! loop ()
        }

    loop ()

let client (mailbox: Actor<_>) =

    let mutable tweetCount = 0
    let mutable connected = 1
    let mutable receivedTweets = List.empty
    let mutable disconnectedTweets = List.empty

    let rec loop () =
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | Tweet (client, numUsers) ->
                if connected = 1 then
                    let random = Random().Next(1, 101)
                    let mutable tweet = ""
                    if random <= 50 then
                        tweet <- sprintf "This is User%i's TweetNo.%i " client tweetCount
                    elif random > 50 && random <= 75 then
                        tweet <-
                            sprintf "This is User%i's TweetNo.%i #Hashtag%i " client tweetCount (Random().Next(1, 11))
                    else
                        tweet <-
                            sprintf
                                "This is User%i's TweetNo.%i @User%i "
                                client
                                tweetCount
                                (Random().Next(1, numUsers + 1))
                    printfn "User%i tweeted." client
                    tweetCount <- tweetCount + 1
                    serverRef <! SendTweetToServer(client, tweet)

            | ReceiveTweetFromServer (client, tweet) ->
                if connected = 0 then
                    disconnectedTweets <- disconnectedTweets @ [ tweet ]
                else
                    receivedTweets <- receivedTweets @ [ tweet ]
                    if connected = 1
                    then printfn "Feed of User%i:\nTweet: %s" client tweet
                    serverRef <! TweetAck

            | Retweet (client, numUsers) ->
                if receivedTweets.Length > 0 then
                    if connected = 1 then
                        let random = Random().Next(0, receivedTweets.Length)
                        let retweet = receivedTweets.[random]
                        printfn "User%i retweeted." client
                        serverRef <! SendRetweetToServer(client, retweet)

            | ReceiveRetweetFromServer (client, retweet) ->
                receivedTweets <- receivedTweets @ [ retweet ]
                if connected = 1
                then printfn "Feed of User%i:\nRetweet: %s" client retweet
                serverRef <! RetweetAck

            | QueryAllRequest (client) -> serverRef <! QueryAll(client)

            | QueryAllResponse (client, allTweets) ->
                printfn "\nTimeline of User%i after AllTweetsQuery" client
                for tweet in allTweets do
                    printfn "%s" tweet

            | QueryHashtagRequest (client) ->
                serverRef
                <! QueryHashtag(client, sprintf " #Hashtag%i " (Random().Next(1, 11)))

            | QueryHashtagResponse (client, hashtag, response) ->
                printfn "\nSearch Results for User%i after searching for %s" client hashtag
                for tweet in response do
                    printfn "%s" tweet

            | QueryMentionRequest (client) ->
                serverRef
                <! QueryMention(client, sprintf " @User%i " (Random().Next(1, numUsers + 1)))

            | QueryMentionResponse (client, mention, response) ->
                printfn "\nSearch Results for User%i after searching for %s" client mention
                for tweet in response do
                    printfn "%s" tweet

            | Disconnect (client) ->
                connected <- 0
                printfn "User%i disconnected." client

            | Connect (client) ->
                connected <- 1
                printfn "User%i connected." client
                printfn "Feed of User%i:" client
                for tweet in disconnectedTweets do
                    printfn "Tweet: %s" tweet
                    receivedTweets <- receivedTweets @ [ tweet ]
                disconnectedTweets <- List.empty

            | _ -> ()

            return! loop ()
        }

    loop ()

let clientParent (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | CreateClients (numUsers) ->
                for i = 1 to numUsers do
                    clientToClientRef <- clientToClientRef.Add(i, spawn system (sprintf "client%i" i) client)
                for i = 1 to numUsers do
                    simulatorRef <! AssignFollowers(i)
                Threading.Thread.Sleep(2000)

            | _ -> ()

            return! loop ()
        }

    loop ()

let simulator (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | Initialize (numUsers) ->
                for i = 1 to numUsers do
                    let mutable random =
                        Random().Next(1, int ((numUsers - i + 2) / 2))

                    if random = 0 then random <- 1

                    clientToNumFollowers <- clientToNumFollowers.Add(i, random)
                    totalNumTweets <- totalNumTweets + double random
                    clientToFollowers <- clientToFollowers.Add(i, Set.empty)
                    clientToFollowing <- clientToFollowing.Add(i, Set.empty)
                    serverRef <! InitializeServer(numUsers)
                totalNumRetweets <- totalNumTweets

            | CreateClients (numUsers) -> clientParentRef <! CreateClients(numUsers)

            | AssignFollowers (client) ->
                let numFollowers = clientToNumFollowers.[client]
                for i = 1 to numFollowers do
                    let mutable random = Random().Next(1, numUsers + 1)
                    while random = client
                          || clientToFollowing.[random].Contains(client) do
                        random <- Random().Next(1, numUsers + 1)
                    let mutable followingSet = clientToFollowing.[random]
                    followingSet <- followingSet.Add(client)
                    clientToFollowing <- clientToFollowing.Add(random, followingSet)
                    let mutable followerSet = clientToFollowers.[client]
                    followerSet <- followerSet.Add(random)
                    clientToFollowers <- clientToFollowers.Add(client, followerSet)

            | CalcNumZero ->
                for i = 1 to numUsers do
                    if clientToFollowing.[i].Count = 0 then
                        numZeroFollowing <-
                            numZeroFollowing
                            + double clientToFollowers.[i].Count

            | InitialTweet (numUsers) ->
                for i = 1 to numUsers do
                    clientToClientRef.[i] <! Tweet(i, numUsers)

            | InitialRetweet (numUsers) ->
                for i = 1 to numUsers do
                    clientToClientRef.[i] <! Retweet(i, numUsers)

            | InitialQueryAll ->
                for i = 1 to numUsers do
                    clientToClientRef.[i] <! QueryAllRequest(i)
                    Threading.Thread.Sleep(100)
                flag1 <- true

            | InitialQueryHashtag ->
                let mutable randomSet = Set.empty
                for i = 1 to 10 do
                    let mutable random = Random().Next(1, numUsers + 1)
                    while randomSet.Contains(random) do
                        random <- Random().Next(1, numUsers + 1)
                    randomSet <- randomSet.Add(random)
                for client in randomSet do
                    clientToClientRef.[client]
                    <! QueryHashtagRequest(client)
                    Threading.Thread.Sleep(100)
                flag1 <- true

            | InitialQueryMention ->
                let mutable randomSet = Set.empty
                for i = 1 to 10 do
                    let mutable random = Random().Next(1, numUsers + 1)
                    while randomSet.Contains(random) do
                        random <- Random().Next(1, numUsers + 1)
                    randomSet <- randomSet.Add(random)
                for client in randomSet do
                    clientToClientRef.[client]
                    <! QueryMentionRequest(client)
                    Threading.Thread.Sleep(100)
                flag1 <- true

            | InitialConnectDisconnect ->
                let random = Random().Next(1, numUsers + 1)
                let mutable disconnectedClients = Set.empty
                for client in clientToFollowers.[random] do
                    let r = Random().Next(1, 101)
                    if r <= 50 then
                        disconnectedClients <- disconnectedClients.Add(client)
                        clientToClientRef.[client] <! Disconnect(client)
                        Threading.Thread.Sleep(500)
                Threading.Thread.Sleep(1000)
                printfn ""
                clientToClientRef.[random]
                <! Tweet(random, numUsers)
                Threading.Thread.Sleep(1000)
                printfn ""
                for client in disconnectedClients do
                    clientToClientRef.[client] <! Connect(client)
                    Threading.Thread.Sleep(500)
                Threading.Thread.Sleep(1000)
                flag1 <- true

            | _ -> ()

            return! loop ()
        }

    loop ()

let arguments = Environment.GetCommandLineArgs()
numUsers <- arguments.[3] |> int

simulatorRef <- spawn system "simulator" simulator
clientParentRef <- spawn system "clientParent" clientParent
serverRef <- spawn system "server" server

let stopwatch = Diagnostics.Stopwatch.StartNew()
stopwatch.Stop()

printfn "\n\nStarting Initialization of Network ..."
simulatorRef <! Initialize(numUsers)
simulatorRef <! CreateClients(numUsers)
Threading.Thread.Sleep(20 * numUsers)
simulatorRef <! CalcNumZero

for i = 1 to numUsers do
    printfn "%A : %i : %A" clientToFollowers.[i] i clientToFollowing.[i]

Threading.Thread.Sleep(5000)

printfn "\n\nStarting Tweeting ..."
temp <- true
flag1 <- false
stopwatch.Start()
simulatorRef <! InitialTweet(numUsers)

while temp do
    //printfn "%i" totalNumTweets
    if totalNumTweets = numTweets then flag1 <- true
    if flag1 then temp <- false

stopwatch.Stop()
let timeTweet = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(5000)

printfn "\n\nStarting Retweeting ..."
temp <- true
flag1 <- false
stopwatch.Start()
simulatorRef <! InitialRetweet(numUsers)

while temp do
    if totalNumRetweets - numZeroFollowing = numRetweets
    then flag1 <- true
    if flag1 then temp <- false

stopwatch.Stop()
let timeRetweet = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(5000)

printfn "\n\nStarting QueryAll ...\n"
temp <- true
flag1 <- false
stopwatch.Start()
simulatorRef <! InitialQueryAll

while temp do
    if flag1 then temp <- false

stopwatch.Stop()
let timeQueryAll = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(2000)

printfn "\n\nStarting QueryHashtag ...\n"
temp <- true
flag1 <- false
stopwatch.Start()
simulatorRef <! InitialQueryHashtag

while temp do
    if flag1 then temp <- false

stopwatch.Stop()
let timeQueryHashtag = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(2000)

printfn "\n\nStarting QueryMention ...\n"
temp <- true
flag1 <- false
stopwatch.Start()
simulatorRef <! InitialQueryMention

while temp do
    if flag1 then temp <- false

stopwatch.Stop()
let timeQueryMention = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(5000)

printfn "\n\nStarting Connection-Disconnection Simulation ..."
temp <- true
flag1 <- false
stopwatch.Start()
simulatorRef <! InitialConnectDisconnect

while temp do
    if flag1 then temp <- false

stopwatch.Stop()
let timeConnectDisconnect = stopwatch.Elapsed.TotalMilliseconds

printfn "\n\nTime Statistics: \n\nTime for Tweeting with %i users and %f tweets: %f\nTime for Retweeting with %i users and %f retweets: %f\nTime for Printing Home Timeline with %i users: %f\nTime for Hashtag Query with 10 users: %f\nTime for Mention Query with 10 users: %f\nTime for Connect Disconnect Simulation: %f"
    numUsers totalNumTweets timeTweet numUsers totalNumRetweets (timeRetweet - timeTweet) numUsers
    (timeQueryAll - timeRetweet) (timeQueryHashtag - timeQueryAll) (timeQueryMention - timeQueryHashtag)
    (timeConnectDisconnect - timeQueryMention)