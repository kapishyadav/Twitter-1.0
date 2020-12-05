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
    | Init of int
    | InitServer of int
    | MakeClients of int
    | AllotFollowers of int
    | FirstTweet of int
    | Tweet of int * int
    | TransmitTweetToServer of int * string
    | TransmitTweetToClient of int * string
    | GetTweetFromServer of int * string
    | Tweet_Acknowledgment
    | FirstRetweet of int
    | Retweet of int * int
    | TransmitRetweetToServer of int * string
    | TransmitRetweetToClient of int * string
    | GetRetweetFromServer of int * string
    | Retweet_Acknowledgment
    | FirstQueryAll
    | AllRequest_Query of int
    | Query_Bulk of int
    | QueryBulkResponse of int * List<string>
    | FirstQueryHashtag
    | GetHashtagRequest of int
    | GetHashtag of int * string
    | GetHashtagResponse of int * string * List<string>
    | FirstQueryMention
    | GetMentionRequest of int
    | GetMention of int * string
    | GetMentionResponse of int * string * List<string>
    | CalculateZero
    | InitConnDisc
    | Disconnect of int
    | Connect of int

let mutable num_of_users = 100

let mutable simulator_reference = null
let mutable client_parent_reference = null
let mutable server_reference = null

let mutable client_to_num_followers: Map<int, int> = Map.empty
let mutable client_to_client_reference: Map<int, IActorRef> = Map.empty
let mutable client_to_followers: Map<int, Set<int>> = Map.empty
let mutable client_to_following: Map<int, Set<int>> = Map.empty

let mutable f1 = false
let mutable temporary = true

let mutable total_tweets = double 0
let mutable number_of_tweets = double 0
let mutable num_zero_following = double 0
let mutable total_retweets = double 0
let mutable num_of_retweets = double 0

let server (mailbox: Actor<_>) =

    let mutable clientToTweets: Map<int, List<string>> = Map.empty

    let rec loop () =
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | InitServer (num_of_users) ->
                for i = 1 to num_of_users do
                    clientToTweets <- clientToTweets.Add(i, List.empty)

            | TransmitTweetToServer (client, tweet) ->
                for follower in client_to_followers.[client] do
                    let mutable allTweets = clientToTweets.[follower]
                    allTweets <- allTweets @ [ sprintf "Tweet: %s" tweet ]
                    clientToTweets <- clientToTweets.Add(follower, allTweets)
                    server_reference <! TransmitTweetToClient(follower, tweet)

            | TransmitTweetToClient (client, tweet) ->
                client_to_client_reference.[client]
                <! GetTweetFromServer(client, tweet)

            | Tweet_Acknowledgment -> number_of_tweets <- number_of_tweets + double 1

            | TransmitRetweetToServer (client, retweet) ->
                for follower in client_to_followers.[client] do
                    let mutable allTweets = clientToTweets.[follower]
                    allTweets <-
                        allTweets
                        @ [ sprintf "Retweet (by User%i): %s" client retweet ]
                    clientToTweets <- clientToTweets.Add(follower, allTweets)
                    server_reference
                    <! TransmitRetweetToClient(follower, retweet)

            | TransmitRetweetToClient (client, retweet) ->
                client_to_client_reference.[client]
                <! GetRetweetFromServer(client, retweet)

            | Retweet_Acknowledgment -> num_of_retweets <- num_of_retweets + double 1

            | Query_Bulk (client) ->
                client_to_client_reference.[client]
                <! QueryBulkResponse(client, clientToTweets.[client])

            | GetHashtag (client, hashtag) ->
                let allTweets = clientToTweets.[client]
                let mutable response = List.empty
                for tweet in allTweets do
                    if tweet.IndexOf(hashtag) <> -1 then response <- response @ [ tweet ]
                client_to_client_reference.[client]
                <! GetHashtagResponse(client, hashtag, response)

            | GetMention (client, mention) ->
                let allTweets = clientToTweets.[client]
                let mutable response = List.empty
                for tweet in allTweets do
                    if tweet.IndexOf(mention) <> -1 then response <- response @ [ tweet ]
                client_to_client_reference.[client]
                <! GetHashtagResponse(client, mention, response)

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
            | Tweet (client, num_of_users) ->
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
                                (Random().Next(1, num_of_users + 1))
                    printfn "User%i tweeted." client
                    tweetCount <- tweetCount + 1
                    server_reference <! TransmitTweetToServer(client, tweet)

            | GetTweetFromServer (client, tweet) ->
                if connected = 0 then
                    disconnectedTweets <- disconnectedTweets @ [ tweet ]
                else
                    receivedTweets <- receivedTweets @ [ tweet ]
                    if connected = 1
                    then printfn "Feed of User%i:\nTweet: %s" client tweet
                    server_reference <! Tweet_Acknowledgment

            | Retweet (client, num_of_users) ->
                if receivedTweets.Length > 0 then
                    if connected = 1 then
                        let random = Random().Next(0, receivedTweets.Length)
                        let retweet = receivedTweets.[random]
                        printfn "User%i retweeted." client
                        server_reference <! TransmitRetweetToServer(client, retweet)

            | GetRetweetFromServer (client, retweet) ->
                receivedTweets <- receivedTweets @ [ retweet ]
                if connected = 1
                then printfn "Feed of User%i:\nRetweet: %s" client retweet
                server_reference <! Retweet_Acknowledgment

            | AllRequest_Query (client) -> server_reference <! Query_Bulk(client)

            | QueryBulkResponse (client, allTweets) ->
                printfn "\nTimeline of User%i after AllTweetsQuery" client
                for tweet in allTweets do
                    printfn "%s" tweet

            | GetHashtagRequest (client) ->
                server_reference
                <! GetHashtag(client, sprintf " #Hashtag%i " (Random().Next(1, 11)))

            | GetHashtagResponse (client, hashtag, response) ->
                printfn "\nSearch Results for User%i after searching for %s" client hashtag
                for tweet in response do
                    printfn "%s" tweet

            | GetMentionRequest (client) ->
                server_reference
                <! GetMention(client, sprintf " @User%i " (Random().Next(1, num_of_users + 1)))

            | GetMentionResponse (client, mention, response) ->
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
            | MakeClients (num_of_users) ->
                for i = 1 to num_of_users do
                    client_to_client_reference <- client_to_client_reference.Add(i, spawn system (sprintf "client%i" i) client)
                for i = 1 to num_of_users do
                    simulator_reference <! AllotFollowers(i)
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
            | Init (num_of_users) ->
                for i = 1 to num_of_users do
                    let mutable random =
                        Random().Next(1, int ((num_of_users - i + 2) / 2))

                    if random = 0 then random <- 1

                    client_to_num_followers <- client_to_num_followers.Add(i, random)
                    total_tweets <- total_tweets + double random
                    client_to_followers <- client_to_followers.Add(i, Set.empty)
                    client_to_following <- client_to_following.Add(i, Set.empty)
                    server_reference <! InitServer(num_of_users)
                total_retweets <- total_tweets

            | MakeClients (num_of_users) -> client_parent_reference <! MakeClients(num_of_users)

            | AllotFollowers (client) ->
                let numFollowers = client_to_num_followers.[client]
                for i = 1 to numFollowers do
                    let mutable random = Random().Next(1, num_of_users + 1)
                    while random = client
                          || client_to_following.[random].Contains(client) do
                        random <- Random().Next(1, num_of_users + 1)
                    let mutable followingSet = client_to_following.[random]
                    followingSet <- followingSet.Add(client)
                    client_to_following <- client_to_following.Add(random, followingSet)
                    let mutable followerSet = client_to_followers.[client]
                    followerSet <- followerSet.Add(random)
                    client_to_followers <- client_to_followers.Add(client, followerSet)

            | CalculateZero ->
                for i = 1 to num_of_users do
                    if client_to_following.[i].Count = 0 then
                        num_zero_following <-
                            num_zero_following
                            + double client_to_followers.[i].Count

            | FirstTweet (num_of_users) ->
                for i = 1 to num_of_users do
                    client_to_client_reference.[i] <! Tweet(i, num_of_users)

            | FirstRetweet (num_of_users) ->
                for i = 1 to num_of_users do
                    client_to_client_reference.[i] <! Retweet(i, num_of_users)

            | FirstQueryAll ->
                for i = 1 to num_of_users do
                    client_to_client_reference.[i] <! AllRequest_Query(i)
                    Threading.Thread.Sleep(100)
                f1 <- true

            | FirstQueryHashtag ->
                let mutable randomSet = Set.empty
                for i = 1 to 10 do
                    let mutable random = Random().Next(1, num_of_users + 1)
                    while randomSet.Contains(random) do
                        random <- Random().Next(1, num_of_users + 1)
                    randomSet <- randomSet.Add(random)
                for client in randomSet do
                    client_to_client_reference.[client]
                    <! GetHashtagRequest(client)
                    Threading.Thread.Sleep(100)
                f1 <- true

            | FirstQueryMention ->
                let mutable randomSet = Set.empty
                for i = 1 to 10 do
                    let mutable random = Random().Next(1, num_of_users + 1)
                    while randomSet.Contains(random) do
                        random <- Random().Next(1, num_of_users + 1)
                    randomSet <- randomSet.Add(random)
                for client in randomSet do
                    client_to_client_reference.[client]
                    <! GetMentionRequest(client)
                    Threading.Thread.Sleep(100)
                f1 <- true

            | InitConnDisc ->
                let random = Random().Next(1, num_of_users + 1)
                let mutable disconnectedClients = Set.empty
                for client in client_to_followers.[random] do
                    let r = Random().Next(1, 101)
                    if r <= 50 then
                        disconnectedClients <- disconnectedClients.Add(client)
                        client_to_client_reference.[client] <! Disconnect(client)
                        Threading.Thread.Sleep(500)
                Threading.Thread.Sleep(1000)
                printfn ""
                client_to_client_reference.[random]
                <! Tweet(random, num_of_users)
                Threading.Thread.Sleep(1000)
                printfn ""
                for client in disconnectedClients do
                    client_to_client_reference.[client] <! Connect(client)
                    Threading.Thread.Sleep(500)
                Threading.Thread.Sleep(1000)
                f1 <- true

            | _ -> ()

            return! loop ()
        }

    loop ()

let arguments = Environment.GetCommandLineArgs()
num_of_users <- arguments.[3] |> int

simulator_reference <- spawn system "simulator" simulator
client_parent_reference <- spawn system "clientParent" clientParent
server_reference <- spawn system "server" server

let stopwatch = Diagnostics.Stopwatch.StartNew()
stopwatch.Stop()

printfn "\n\nStarting Initialization of Network ..."
simulator_reference <! Init(num_of_users)
simulator_reference <! MakeClients(num_of_users)
Threading.Thread.Sleep(20 * num_of_users)
simulator_reference <! CalculateZero

for i = 1 to num_of_users do
    printfn "%A : %i : %A" client_to_followers.[i] i client_to_following.[i]

Threading.Thread.Sleep(5000)

printfn "\n\nStarting Tweeting ..."
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! FirstTweet(num_of_users)

while temporary do
    //printfn "%i" total_tweets
    if total_tweets = number_of_tweets then f1 <- true
    if f1 then temporary <- false

stopwatch.Stop()
let timeTweet = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(5000)

printfn "\n\nStarting Retweeting ..."
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! FirstRetweet(num_of_users)

while temporary do
    if total_retweets - num_zero_following = num_of_retweets
    then f1 <- true
    if f1 then temporary <- false

stopwatch.Stop()
let timeRetweet = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(5000)

printfn "\n\nStarting Query_Bulk ...\n"
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! FirstQueryAll

while temporary do
    if f1 then temporary <- false

stopwatch.Stop()
let timeQueryAll = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(2000)

printfn "\n\nStarting GetHashtag ...\n"
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! FirstQueryHashtag

while temporary do
    if f1 then temporary <- false

stopwatch.Stop()
let timeQueryHashtag = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(2000)

printfn "\n\nStarting GetMention ...\n"
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! FirstQueryMention

while temporary do
    if f1 then temporary <- false

stopwatch.Stop()
let timeQueryMention = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(5000)

printfn "\n\nStarting Connection-Disconnection Simulation ..."
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! InitConnDisc

while temporary do
    if f1 then temporary <- false

stopwatch.Stop()
let timeConnectDisconnect = stopwatch.Elapsed.TotalMilliseconds

printfn "\n\nTime Statistics: \n\nTime for Tweeting with %i users and %f tweets: %f\nTime for Retweeting with %i users and %f retweets: %f\nTime for Printing Home Timeline with %i users: %f\nTime for Hashtag Query with 10 users: %f\nTime for Mention Query with 10 users: %f\nTime for Connect Disconnect Simulation: %f"
    num_of_users total_tweets timeTweet num_of_users total_retweets (timeRetweet - timeTweet) num_of_users
    (timeQueryAll - timeRetweet) (timeQueryHashtag - timeQueryAll) (timeQueryMention - timeQueryHashtag)
    (timeConnectDisconnect - timeQueryMention)