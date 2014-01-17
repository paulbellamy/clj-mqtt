(ns mqtt.example.client
  (:require [clojure.core.async :as async]
            [mqtt.encoder :as encoder]
            [mqtt.decoder :as decoder])
  (:import [java.net InetSocketAddress]
           [io.netty.bootstrap Bootstrap]
           [io.netty.channel.nio NioEventLoopGroup]
           [io.netty.channel.socket.nio NioSocketChannel]
           [io.netty.channel Channel SimpleChannelInboundHandler ChannelInitializer ChannelHandler ChannelHandlerContext ChannelFuture]
           [com.xively.netty Netty]))

(defn- start-sender [sock channel]
  (async/go-loop []
    (try
      (let [msg (async/<! (:out sock))]
        (when-not (nil? msg)
          (let [[done msg] msg]
            (Netty/write channel msg)
            (Netty/flush channel)
            (async/close! done))
          (recur)))
     (catch Exception e e))))

(declare close)
(defn- gen-response-handler [sock f]
  (proxy [SimpleChannelInboundHandler] []
    (channelRead0 [ctx msg]
      (f msg))

    (channelInactive [ctx]
      (close (assoc sock :future ctx)))

    (exceptionCaught
      [^ChannelHandlerContext ctx cause]
      (close sock)
      (throw cause))))

(defn- gen-channel-initializer
  [sock ^ChannelHandler handler]
  (proxy [ChannelInitializer] []
    (initChannel [^Channel ch]
      (start-sender sock ch)
      (doto (Netty/pipeline ch)
        (.addLast "encoder" ^ChannelHandler (encoder/make-encoder))
        (.addLast "decoder" ^ChannelHandler (decoder/make-decoder))
        (.addLast "handler" handler)))))

(defn socket
  "Creates and connects a raw mqtt socket to the address. Returns two chans an input one and an output one."
  [addr]
  (let [uri (java.net.URI. addr)
        sock {:in (async/chan)
              :out (async/chan)
              :next-id (atom 0)}
        bootstrap (Bootstrap.)]
    (Netty/group bootstrap (NioEventLoopGroup.))
    (Netty/clientChannel bootstrap NioSocketChannel)
    (Netty/handler bootstrap
      (gen-channel-initializer sock
        (gen-response-handler sock
          (fn [msg]
            (async/put! (:in sock) msg)))))
    (assoc sock :future (-> (.connect bootstrap (.getHost uri) (.getPort uri))
                            (.await)))))

(defn- get-next-message-id [socket]
  (swap! (:next-id socket) #(-> % inc (mod Short/MAX_VALUE))))

(defn recv-message
  [socket]
  (async/<!! (:in socket)))

(defn send-message
  "Send a packet from the client. We use the 'done' channel as a mutex so we
  can block until the send is done."
  [socket msg]
  (let [done (async/chan)]
    (async/>!! (:out socket) [done msg])
    (async/<!! done)))

(defn expect-message
  [socket f & {:keys [timeout]}]
  (let [msg (first (async/alts!! [(:in socket) (async/timeout (or timeout 1000))]))]
    (cond
     (nil? msg) (throw (Exception. "No message received"))
     (not (f msg)) (throw (Exception. "Unexpected message received" msg))
     :else msg)))

(defn connect
  [socket opts]
  (send-message socket (merge {:type :connect
                               :client-id (str (gensym))}
                              opts))
  (expect-message socket #(= :accepted (:return-code %))))

(defn publish-and-wait
  [socket topic payload]
  (let [message-id (get-next-message-id socket)]
    (send-message socket {:type :publish
                          :topic topic
                          :payload payload
                          :qos 1
                          :message-id message-id})
    (expect-message socket #(and (= :puback (:type %))
                                 (= message-id (:message-id %))))))

(defn ping
  [socket]
  (send-message socket {:type :pingreq})
  (expect-message socket #(= :pingresp (:type %))))

(defn connected?
  "Check if a socket is currently connected."
  [socket]
  (try
    (.isActive (.channel ^ChannelFuture (:future socket)))
    (catch Exception e false)))

(defn disconnected?
  "Check if a socket receives no more messages and is disconnected."
  [socket]
  (let [msg (async/<!! (:in socket))]
    (if (nil? msg)
      (not (connected? socket))
      (throw (Exception. "Received unexpected message" msg)))))

(defn close
  [socket]
  (.await (Netty/close (.channel (:future socket))))
  (async/close! (:out socket))
  (async/close! (:in socket)))

(defn disconnect
  [socket]
  (send-message socket {:type :disconnect})
  (close socket))
