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

    let mutable client_to_tweets: Map<int, List<string>> = Map.empty

    let rec loop () =
        actor {
            let! message = mailbox.Receive()

            match message with
            | InitServer (num_of_users) ->
                for i = 1 to num_of_users do
                    client_to_tweets <- client_to_tweets.Add(i, List.empty)

            | TransmitTweetToServer (client, tweet) ->
                for follower in client_to_followers.[client] do
                    let mutable every_tweet = client_to_tweets.[follower]
                    every_tweet <- every_tweet @ [ sprintf "Tweet: %s" tweet ]
                    client_to_tweets <- client_to_tweets.Add(follower, every_tweet)
                    server_reference <! TransmitTweetToClient(follower, tweet)

            | TransmitTweetToClient (client, tweet) ->
                client_to_client_reference.[client]
                <! GetTweetFromServer(client, tweet)

            | Tweet_Acknowledgment -> number_of_tweets <- number_of_tweets + double 1

            | TransmitRetweetToServer (client, retweet) ->
                for follower in client_to_followers.[client] do
                    let mutable every_tweet = client_to_tweets.[follower]
                    every_tweet <-
                        every_tweet
                        @ [ sprintf "Retweet (by User%i): %s" client retweet ]
                    client_to_tweets <- client_to_tweets.Add(follower, every_tweet)
                    server_reference
                    <! TransmitRetweetToClient(follower, retweet)

            | TransmitRetweetToClient (client, retweet) ->
                client_to_client_reference.[client]
                <! GetRetweetFromServer(client, retweet)

            | Retweet_Acknowledgment -> num_of_retweets <- num_of_retweets + double 1

            | Query_Bulk (client) ->
                client_to_client_reference.[client]
                <! QueryBulkResponse(client, client_to_tweets.[client])

            | GetHashtag (client, hashtag) ->
                let every_tweet = client_to_tweets.[client]
                let mutable response = List.empty
                for tweet in every_tweet do
                    if tweet.IndexOf(hashtag) <> -1 then response <- response @ [ tweet ]
                client_to_client_reference.[client]
                <! GetHashtagResponse(client, hashtag, response)

            | GetMention (client, mention) ->
                let every_tweet = client_to_tweets.[client]
                let mutable response = List.empty
                for tweet in every_tweet do
                    if tweet.IndexOf(mention) <> -1 then response <- response @ [ tweet ]
                client_to_client_reference.[client]
                <! GetHashtagResponse(client, mention, response)

            | _ -> ()

            return! loop ()
        }

    loop ()

let client (mailbox: Actor<_>) =

    let mutable count_of_tweets = 0
    let mutable conn = 1
    let mutable tweets_received = List.empty
    let mutable tweets_disconnected = List.empty

    let rec loop () =
        actor {
            let! message = mailbox.Receive()

            match message with
            | Tweet (client, num_of_users) ->
                if conn = 1 then
                    let rndm = Random().Next(1, 101)
                    let mutable tweet = ""
                    if rndm <= 50 then
                        tweet <- sprintf "This is User%i's TweetNo.%i " client count_of_tweets
                    elif rndm > 50 && rndm <= 75 then
                        tweet <-
                            sprintf "This is User%i's TweetNo.%i #Hashtag%i " client count_of_tweets (Random().Next(1, 11))
                    else
                        tweet <-
                            sprintf
                                "This is User%i's TweetNo.%i @User%i "
                                client
                                count_of_tweets
                                (Random().Next(1, num_of_users + 1))
                    printfn "User%i tweeted." client
                    count_of_tweets <- count_of_tweets + 1
                    server_reference <! TransmitTweetToServer(client, tweet)

            | GetTweetFromServer (client, tweet) ->
                if conn = 0 then
                    tweets_disconnected <- tweets_disconnected @ [ tweet ]
                else
                    tweets_received <- tweets_received @ [ tweet ]
                    if conn = 1
                    then printfn "Feed of User%i:\nTweet: %s" client tweet
                    server_reference <! Tweet_Acknowledgment

            | Retweet (client, num_of_users) ->
                if tweets_received.Length > 0 then
                    if conn = 1 then
                        let rndm = Random().Next(0, tweets_received.Length)
                        let retweet = tweets_received.[rndm]
                        printfn "User%i retweeted." client
                        server_reference <! TransmitRetweetToServer(client, retweet)

            | GetRetweetFromServer (client, retweet) ->
                tweets_received <- tweets_received @ [ retweet ]
                if conn = 1
                then printfn "Feed of User%i:\nRetweet: %s" client retweet
                server_reference <! Retweet_Acknowledgment

            | AllRequest_Query (client) -> server_reference <! Query_Bulk(client)

            | QueryBulkResponse (client, every_tweet) ->
                printfn "\nTimeline of User%i after AllTweetsQuery" client
                for tweet in every_tweet do
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
                conn <- 0
                printfn "User%i disconnected." client

            | Connect (client) ->
                conn <- 1
                printfn "User%i conn." client
                printfn "Feed of User%i:" client
                for tweet in tweets_disconnected do
                    printfn "Tweet: %s" tweet
                    tweets_received <- tweets_received @ [ tweet ]
                tweets_disconnected <- List.empty

            | _ -> ()

            return! loop ()
        }

    loop ()

let clientParent (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! message = mailbox.Receive()

            match message with
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
            let! message = mailbox.Receive()

            match message with
            | Init (num_of_users) ->
                for i = 1 to num_of_users do
                    let mutable rndm =
                        Random().Next(1, int ((num_of_users - i + 2) / 2))

                    if rndm = 0 then rndm <- 1

                    client_to_num_followers <- client_to_num_followers.Add(i, rndm)
                    total_tweets <- total_tweets + double rndm
                    client_to_followers <- client_to_followers.Add(i, Set.empty)
                    client_to_following <- client_to_following.Add(i, Set.empty)
                    server_reference <! InitServer(num_of_users)
                total_retweets <- total_tweets

            | MakeClients (num_of_users) -> client_parent_reference <! MakeClients(num_of_users)

            | AllotFollowers (client) ->
                let number_of_followers = client_to_num_followers.[client]
                for i = 1 to number_of_followers do
                    let mutable rndm = Random().Next(1, num_of_users + 1)
                    while rndm = client
                          || client_to_following.[rndm].Contains(client) do
                        rndm <- Random().Next(1, num_of_users + 1)
                    let mutable set_of_following = client_to_following.[rndm]
                    set_of_following <- set_of_following.Add(client)
                    client_to_following <- client_to_following.Add(rndm, set_of_following)
                    let mutable set_of_followers = client_to_followers.[client]
                    set_of_followers <- set_of_followers.Add(rndm)
                    client_to_followers <- client_to_followers.Add(client, set_of_followers)

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
                let mutable rndm_set = Set.empty
                for i = 1 to 10 do
                    let mutable rndm = Random().Next(1, num_of_users + 1)
                    while rndm_set.Contains(rndm) do
                        rndm <- Random().Next(1, num_of_users + 1)
                    rndm_set <- rndm_set.Add(rndm)
                for client in rndm_set do
                    client_to_client_reference.[client]
                    <! GetHashtagRequest(client)
                    Threading.Thread.Sleep(100)
                f1 <- true

            | FirstQueryMention ->
                let mutable rndm_set = Set.empty
                for i = 1 to 10 do
                    let mutable rndm = Random().Next(1, num_of_users + 1)
                    while rndm_set.Contains(rndm) do
                        rndm <- Random().Next(1, num_of_users + 1)
                    rndm_set <- rndm_set.Add(rndm)
                for client in rndm_set do
                    client_to_client_reference.[client]
                    <! GetMentionRequest(client)
                    Threading.Thread.Sleep(100)
                f1 <- true

            | InitConnDisc ->
                let rndm = Random().Next(1, num_of_users + 1)
                let mutable clients_disconnected = Set.empty
                for client in client_to_followers.[rndm] do
                    let r = Random().Next(1, 101)
                    if r <= 50 then
                        clients_disconnected <- clients_disconnected.Add(client)
                        client_to_client_reference.[client] <! Disconnect(client)
                        Threading.Thread.Sleep(500)
                Threading.Thread.Sleep(1000)
                printfn ""
                client_to_client_reference.[rndm]
                <! Tweet(rndm, num_of_users)
                Threading.Thread.Sleep(1000)
                printfn ""
                for client in clients_disconnected do
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
let retweet_time = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(5000)

printfn "\n\nStarting Query_Bulk ...\n"
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! FirstQueryAll

while temporary do
    if f1 then temporary <- false

stopwatch.Stop()
let query_all_time = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(2000)

printfn "\n\nStarting GetHashtag ...\n"
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! FirstQueryHashtag

while temporary do
    if f1 then temporary <- false

stopwatch.Stop()
let hashtag_query_time = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(2000)

printfn "\n\nStarting GetMention ...\n"
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! FirstQueryMention

while temporary do
    if f1 then temporary <- false

stopwatch.Stop()
let query_mention_time = stopwatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(5000)

printfn "\n\nStarting Connection-Disconnection Simulation ..."
temporary <- true
f1 <- false
stopwatch.Start()
simulator_reference <! InitConnDisc

while temporary do
    if f1 then temporary <- false

stopwatch.Stop()
let conn_disconn_time = stopwatch.Elapsed.TotalMilliseconds

printfn "\n\nTime Statistics: \n\nTime for Tweeting with %i users and %f tweets: %f\nTime for Retweeting with %i users and %f retweets: %f\nTime for Printing Home Timeline with %i users: %f\nTime for Hashtag Query with 10 users: %f\nTime for Mention Query with 10 users: %f\nTime for Connect Disconnect Simulation: %f"
    num_of_users total_tweets timeTweet num_of_users total_retweets (retweet_time - timeTweet) num_of_users
    (query_all_time - retweet_time) (hashtag_query_time - query_all_time) (query_mention_time - hashtag_query_time)
    (conn_disconn_time - query_mention_time)