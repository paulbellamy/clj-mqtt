(ns mqtt.test-client.websockets
  (:import [io.netty.util CharsetUtil]
           [io.netty.channel ChannelHandler ChannelHandlerContext SimpleChannelInboundHandler]
           [io.netty.handler.codec.http DefaultHttpHeaders]
           [io.netty.handler.codec.http.websocketx WebSocketClientHandshakerFactory WebSocketVersion]))

(defn websocket-client-handler
  ^ChannelHandler [uri]
  (let [handshaker (WebSocketClientHandshakerFactory/newHandshaker uri
                                                                   WebSocketVersion/V13
                                                                   "mqttv3.1"
                                                                   true
                                                                   (DefaultHttpHeaders.))]
    (proxy [SimpleChannelInboundHandler] []
      (channelActive [^ChannelHandlerContext ctx]
        (.handshake handshaker (.channel ctx)))
      (channelRead0 [^ChannelHandlerContext ctx ^Object msg]
        (let [ch (.channel ctx)]
          (if-not (.isHandshakeComplete handshaker)
            (.finishHandshake handshaker ch msg)
            (case msg
              FullHttpResponse
              (throw (Exception. (str "Unexpected FullHttpResponse (getStatus=" (.getStatus msg) ", content=" (-> msg .content (.toString CharsetUtil/UTF_8)) ")")))

              TextWebSocketFrame
              (println "got text data")

              BinaryWebSocketFrame
              (println "got binary data")

              PongWebSocketFrame
              (println "got pong")

              CloseWebSocketFrame
              (.close ch)
              )))))))
