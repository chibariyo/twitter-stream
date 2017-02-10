package actors

import akka.actor.{Actor, ActorRef, Props}
import play.api.{Logger, Play}
import play.api.libs.iteratee.{Concurrent, Cont, Done, Enumeratee, Enumerator, Input, Iteratee}
import play.api.libs.iteratee.Concurrent.Broadcaster
import play.api.libs.json.JsObject
import play.api.libs.oauth.{ConsumerKey, OAuthCalculator, RequestToken}
import play.api.libs.ws.WS
import play.api.Play.current
import play.extras.iteratees.{Encoding, JsonIteratees}
import play.api.libs.concurrent.Execution.Implicits._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by david on 17/02/09.
  */
class TwitterStreamer(out: ActorRef) extends Actor {
  def receive = {
    case "subscribe" =>
      Logger.info("Received subscription from a client")
      TwitterStreamer.subscribe(out)
  }

  override def postStop(): Unit = {
    Logger.info("Client unsubscribing from stream")
    TwitterStreamer.unsubscribe(out)
  }
}

object TwitterStreamer {
  def props(out: ActorRef) = Props(new TwitterStreamer(out))

  private var broadcastEnumerator: Option[Enumerator[JsObject]] = None

  private var broadcaster: Option[Broadcaster] = None

  private var subscribers = new ArrayBuffer[ActorRef]()

  def init(): Unit = {
    credentials.map { case (consumerKey, requestToken) =>
      val (iteratee, enumerator) = Concurrent.joined[Array[Byte]]

      val jsonStream: Enumerator[JsObject] = enumerator &>
        Encoding.decode() &>
        Enumeratee.grouped(JsonIteratees.jsSimpleObject)

      val (be, b) = Concurrent.broadcast(jsonStream)
      broadcastEnumerator = Some(be)
      broadcaster = Some(b)

      val maybeMasterNodeUrl = Option(System.getProperty("masterNodeUrl"))
      val url = maybeMasterNodeUrl.getOrElse {
        "https://stream.twitter.com/1.1/statuses/filter.json"
      }
      WS.url(url)
        .sign(OAuthCalculator(consumerKey, requestToken))
        .withQueryString("track" -> "cat")
        .get { response =>
          Logger.info("Status: " + response.status)
          iteratee
        }.map { _ =>
        Logger.info("Twitter stream closed")
      }
    } getOrElse {
      Logger.error("Twitter credentials missing")
    }

  }

  def credentials: Option[(ConsumerKey, RequestToken)] = for {
    apiKey <- Play.configuration.getString("twitter.apiKey")
    apiSecret <- Play.configuration.getString("twitter.apiSecret")
    token <- Play.configuration.getString("twitter.token")
    tokenSecret <- Play.configuration.getString("twitter.tokenSecret")
  } yield (ConsumerKey(apiKey, apiSecret), RequestToken(token, tokenSecret))

  def subscribe(out: ActorRef): Unit = {
    if (broadcastEnumerator.isEmpty) {
      init()
    }

    def twitterClient: Iteratee[JsObject, Unit] = Cont {
      case in@Input.EOF => Done(None)
      case in@Input.El(o) =>
        if (subscribers.contains(out)) {
          out ! o
          twitterClient
        } else {
          Done(None)
        }
      case in@Input.Empty =>
        twitterClient
    }

    broadcastEnumerator.foreach { enumerator =>
      enumerator run twitterClient
    }
    subscribers += out
  }

  def unsubscribe(subscriber: ActorRef): Unit = {
    val index = subscribers.indexWhere(_ == subscriber)
    if (index > 0) {
      subscribers.remove(index)
      Logger.info("Unsubscribed client from stream")
    }
  }

  def subscribeNode: Enumerator[JsObject] = {
    if (broadcastEnumerator.isEmpty) {
      TwitterStreamer.init()
    }
    broadcastEnumerator.getOrElse {
      Enumerator.empty[JsObject]
    }
  }
}
