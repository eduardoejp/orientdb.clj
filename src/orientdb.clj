(ns orientdb
  (:require [clojure.core.async :as async :refer [<! >! go alt!]]
            [clojure.template :refer [do-template]])
  (:import (java.nio ByteBuffer)
           (java.nio.channels AsynchronousSocketChannel
                              CompletionHandler)
           (java.net InetSocketAddress)
           (javax.xml.bind DatatypeConverter)
           (java.util Date)))

;; [Declarations]
(declare read-odoc*
         write-odoc
         document document?
         blob)

(defprotocol _DBConnection
  (_tx [self] [self x])
  (inc-tx-counter! [self]))
(deftype DBConnection [socket session-id clusters cluster-config release ^:unsynchronized-mutable tx ^:unsynchronized-mutable tx-counter]
  _DBConnection
  (_tx [self] tx)
  (_tx [self x] (set! tx x))
  (inc-tx-counter! [self]
    (set! tx-counter (inc tx-counter))
    tx-counter))

(defrecord OCluster [id name type data-segment-id])

(defprotocol _ORID
  (temp-id? [self])
  (cluster-id [self] [self x])
  (cluster-position [self] [self x]))

(deftype ORID [^:unsynchronized-mutable cluster-id ^:unsynchronized-mutable cluster-position]
  _ORID
  (temp-id? [self] (neg? cluster-id))
  (cluster-id [self] cluster-id)
  (cluster-id [self x] (set! cluster-id x))
  (cluster-position [self] cluster-position)
  (cluster-position [self x] (set! cluster-position x))
  clojure.lang.Seqable
  (seq [self] (list cluster-id cluster-position))
  Object
  (toString [self]
    (str "#" cluster-id ":" cluster-position))
  (equals [self o]
    (and (instance? ORID o)
         (= cluster-id (.cluster-id ^ORID o))
         (= cluster-position (.cluster-position ^ORID o))))
  (hashCode [self]
    (+ (* cluster-id Short/MAX_VALUE)
       cluster-position)))

(defprotocol _Tx
  (payload [self] [self x])
  (temp-id! [self]))

(deftype OOptimisticTx [id ^:unsynchronized-mutable temp-position ^:unsynchronized-mutable payload]
  _Tx
  (payload [self] payload)
  (payload [self x] (set! payload x))
  (temp-id! [self]
    (set! temp-position (dec temp-position))
    (ORID. (short -1) temp-position)))

(deftype ConnectionPool [acquire-chan release-chan close-chan config max-conns])

;; [Constants]
(def ^:const ^String +driver-name+ "OrientDB Clojure client")
(def ^:const ^String +driver-version+ "1.0.0")
(def +supported-protocol-versions+ #{17})
(def +protocol-version+ (short 17))
(def ^:const ^String +db-type+ "graph")
(def +byte-array-class+ (class (byte-array 0)))

(def ^:const ^Byte CONNECT (byte 2))
(def ^:const ^Byte DB_OPEN (byte 3))
(def ^:const ^Byte DB_CLOSE (byte 5))
(def ^:const ^Byte DB_SIZE (byte 8))
(def ^:const ^Byte DB_COUNTRECORDS (byte 9))
(def ^:const ^Byte RECORD_LOAD (byte 30))
(def ^:const ^Byte RECORD_CREATE (byte 31))
(def ^:const ^Byte RECORD_UPDATE (byte 32))
(def ^:const ^Byte RECORD_DELETE (byte 33))
(def ^:const ^Byte COMMAND (byte 41))
(def ^:const ^Byte TX_COMMIT (byte 60))

;; [Utils]
(defmacro cond-let [& pairs]
  {:pre [(>= (count pairs) 2)]}
  (reduce (fn [inner form]
            (if (= 1 (count form))
              (first form)
              (let [[binding body] form]
                `(if-let ~binding
                   ~body
                   ~inner))))
          nil (reverse (partition-all 2 pairs))))

(defn ^:private read-value [^String text]
  (let [next-pair (int (.indexOf text ","))
        done? (= -1 next-pair)]
    (let [end-idx (if done?
                    (unchecked-dec-int (.length text))
                    (unchecked-dec-int next-pair))
          remaining (if done?
                      ""
                      (.substring text (unchecked-inc-int next-pair)))]
      (cond (= \" (char (.charAt text 0)))
            [remaining (read-string (.substring text 0 (unchecked-inc-int end-idx)))]

            (let [c (char (.charAt text 0))] (or (= \t c) (= \f c)))
            [remaining (Boolean/parseBoolean (.substring text 0 end-idx))]

            (= \# (char (.charAt text 0)))
            (let [sep (int (.indexOf text ":"))]
              [remaining (ORID. (Short/parseShort (.substring text 1 sep))
                                (Long/parseLong (.substring text (unchecked-inc-int sep) (unchecked-inc-int end-idx))))])

            (= \_ (char (.charAt text 0)))
            [remaining (DatatypeConverter/parseBase64Binary (.substring text 1 end-idx))]

            (= \[ (char (.charAt text 0)))
            (let [_end (int (.indexOf text "]"))
                  string (.substring text 1 _end)]
              (loop [total (transient [])
                     _offset (int 0)
                     _comma (int (.indexOf string ","))]
                (if (= -1 _comma)
                  [(if (> (unchecked-dec-int (.length text)) _end)
                     (.substring text (unchecked-inc-int _end))
                     (.substring text _end))
                   (let [sub (.substring string _offset)
                         total* (if (empty? sub)
                                  total
                                  (conj! total (second (read-value (.substring string _offset)))))]
                     (persistent! total*))]
                  (let [[_ value] (read-value (.substring string _offset _comma))]
                    (recur (conj! total value) (unchecked-inc-int _comma) (.indexOf string "," (unchecked-inc-int _comma)))))))
            
            (= \< (char (.charAt text 0)))
            (let [_end (int (.indexOf text ">"))
                  string (.substring text 1 _end)]
              (loop [total (transient #{})
                     _offset (int 0)
                     _comma (int (.indexOf string ","))]
                (if (= -1 _comma)
                  [(if (> (unchecked-dec-int (.length text)) _end)
                     (.substring text (unchecked-inc-int _end))
                     (.substring text _end))
                   (let [sub (.substring string _offset)
                         total* (if (empty? sub)
                                  total
                                  (conj! total (second (read-value (.substring string _offset)))))]
                     (persistent! total*))]
                  (let [[_ value] (read-value (.substring string _offset _comma))]
                    (recur (conj! total value) (unchecked-inc-int _comma) (.indexOf string "," (unchecked-inc-int _comma)))))))

            (= \{ (char (.charAt text 0)))
            (let [_end (int (.indexOf text "}"))
                  string (.substring text 1 _end)]
              (loop [total (transient {})
                     _offset (int 0)
                     _comma (int (.indexOf string ","))]
                (if (= -1 _comma)
                  [(if (> (unchecked-dec-int (.length text)) _end)
                     (.substring text (unchecked-inc-int _end))
                     (.substring text _end))
                   (let [_colon (int (.indexOf string ":" _offset))
                         [_ key] (read-value (.substring string _offset _colon))
                         [_ value] (read-value (.substring string (unchecked-inc-int _colon)))]
                     (persistent! (assoc! total key value)))]
                  (let [_colon (int (.indexOf string ":" _offset))
                        [_ key] (read-value (.substring string _offset _colon))
                        [_ value] (read-value (.substring string (unchecked-inc-int _colon) _comma))]
                    (recur (assoc! total key value) (unchecked-inc-int _comma) (.indexOf string "," (unchecked-inc-int _comma)))))))

            (= \( (char (.charAt text 0)))
            (let [[region offset] (loop [_last-inner (int 0)
                                         _next-inner (int (.indexOf text "(" 1))
                                         _end (int (.indexOf text ")"))]
                                    (if (< _last-inner _next-inner _end)
                                      (recur _next-inner (.indexOf text "(" _next-inner) (.indexOf text ")" _next-inner))
                                      [(.substring text 1 _end) (unchecked-inc-int _end)]))
                  [_ odoc] (read-odoc* region)]
              [(if (> (.length text) offset)
                 (.substring text offset)
                 text)
               odoc])

            (= \? (char (.charAt text 0)))
            [remaining nil]
            
            (= \b (char (.charAt text end-idx)))
            [remaining (Byte/parseByte (.substring text 0 end-idx))]

            (= \s (char (.charAt text end-idx)))
            [remaining (Short/parseShort (.substring text 0 end-idx))]

            (= \l (char (.charAt text end-idx)))
            [remaining (Long/parseLong (.substring text 0 end-idx))]

            (= \f (char (.charAt text end-idx)))
            [remaining (Float/parseFloat (.substring text 0 end-idx))]

            (= \d (char (.charAt text end-idx)))
            [remaining (Double/parseDouble (.substring text 0 end-idx))]

            (= \c (char (.charAt text end-idx)))
            [remaining (java.math.BigDecimal. (.substring text 0 end-idx))]
            
            (let [c (char (.charAt text end-idx))]
              (or (= \t c) (= \a c)))
            [remaining (java.util.Date. (Long/parseLong (.substring text 0 end-idx)))]

            :else
            [remaining (Integer/parseInt (.substring text 0 (inc end-idx)))]))))

(defn ^:private read-odoc* [^String string]
  (let [_at (int (.indexOf string "@"))
        [class ^String content] (if (or (= -1 _at)
                                        (< -1 (int (.indexOf string ":")) _at))
                                  [nil string]
                                  [(.substring string 0 _at)
                                   (.substring string (unchecked-inc-int _at))])]
    (loop [odoc (document class {})
           remaining content]
      (let [_colon (int (.indexOf remaining ":"))]
        (if (= -1 _colon)
          [remaining odoc]
          (let [_comma (int (.indexOf remaining ","))
                field (if (and (not= -1 _comma)
                               (< _comma _colon))
                        (let [temp (.substring remaining 0 _colon)]
                          (.substring temp (inc (.lastIndexOf temp ",")) (.length temp)))
                        (.substring remaining 0 _colon))
                [remaining* value] (read-value (.substring remaining (unchecked-inc-int _colon)))]
            (recur (assoc odoc field value) remaining*)))))))

(defn ^:private read-odoc [^String odoc]
  (second (read-odoc* (.trim odoc))))

(defn ^:private ^String write-value [val]
  (if (document? val)
    (str "(" (write-odoc val) ")")
    (condp instance? val
      java.lang.Character (str val)
      java.lang.Boolean (str val)
      java.lang.Byte (.concat (str val) "b")
      java.lang.Short (.concat (str val) "s")
      java.lang.Integer (str val)
      java.lang.Long (.concat (str val) "l")
      java.math.BigDecimal (.concat (str val) "c")
      java.lang.Float (.concat (str val) "f")
      java.lang.Double (.concat (str val) "d")
      java.util.Date (.concat (str (.getTime ^Date val)) "t")
      java.lang.String (pr-str val)
      ORID (str val)
      +byte-array-class+ (.concat "_" (DatatypeConverter/printBase64Binary val))
      java.util.List (.concat (if (empty? val)
                                "["
                                (let [^String val* (loop [total (-> "[" (.concat (write-value (first val))))
                                                          elems (next val)]
                                                     (if elems
                                                       (let [e (first elems)]
                                                         (recur (-> total (.concat ",") (.concat (write-value e)))
                                                                (next elems)))
                                                       total))]
                                  val*))
                              "]")
      java.util.Set (.concat (if (empty? val)
                               "<"
                               (let [^String val* (loop [total (-> "<" (.concat (write-value (first val))))
                                                         elems (next val)]
                                                    (if elems
                                                      (let [e (first elems)]
                                                        (recur (-> total (.concat ",") (.concat (write-value e)))
                                                               (next elems)))
                                                      total))]
                                 val*))
                             ">")
      java.util.Map (.concat (if (empty? val)
                               "{"
                               (let [[k v] (first val)
                                     ^String val* (loop [total (-> "{" (.concat (write-value k)) (.concat ":") (.concat (write-value v)))
                                                         elems (next val)]
                                                    (if elems
                                                      (let [[k v] (first elems)]
                                                        (recur (-> total (.concat ",") (.concat (write-value k)) (.concat ":") (.concat (write-value v)))
                                                               (next elems)))
                                                      total))]
                                 val*))
                             "}")
      (if (nil? val)
        "null"
        (throw (ex-info "Invalid value" {:value val}))))
    ))

(defn ^:private ^String write-odoc [odoc]
  (let [init (if-let [oclass (-> odoc meta :class)]
               (str oclass "@")
               "")]
    (if (empty? odoc)
      init
      (let [[k v] (first odoc)]
        (loop [total (-> init (.concat k) (.concat ":") (.concat (write-value v)))
               kvs (next odoc)]
          (if kvs
            (let [[k v] (first kvs)]
              (recur (-> total (.concat ",") (.concat k) (.concat ":") (.concat (write-value v)))
                     (next kvs)))
            total))))))

(defn ^:private ^String prepare-sql-params [params]
  (cond (map? params)
        (write-odoc (document nil {"params" params}))

        (vector? params)
        (write-odoc (document nil {"params" (let [size (count params)]
                                              (loop [key 0
                                                     total (transient {})]
                                                (if (< key size)
                                                  (recur (inc key) (assoc! total (int key) (nth params key)))
                                                  (persistent! total))))}))
        
        (nil? params)
        nil))

(defmacro ^:private return! [chan val]
  `(let [val# ~val]
     (if (nil? val#)
       (clojure.core.async/close! ~chan)
       (doto ~chan
         (clojure.core.async/put! val#)
         clojure.core.async/close!))))

(defmacro ^:private ->callback [chan args & body]
  {:pre [(= 3 (count args))]}
  `(reify CompletionHandler
     (~'completed ~args
       ~@body)
     (~'failed [self# err# _#]
       (return! ~chan err#))))

(defn ^:private ->request [request]
  (let [g!buffer (gensym "buffer")
        lengths (map (fn [[type datum]]
                       (case type
                         :byte 1
                         :char 1
                         :short 2
                         :int 4
                         :long 8
                         :bytes `(+ 4 (alength ~(with-meta datum {:tag "[B"})))
                         :string `(+ 4 (alength (.getBytes ~datum)))
                         :params* `(let [datum# ~datum] (+ 4 (if datum# (alength (.getBytes datum#)) 0)))
                         ))
                     request)
        puts (map (fn [[type datum]]
                    (case type
                      :byte `(.put ~g!buffer ~datum)
                      :char `(.put ~g!buffer (byte ~datum))
                      :short `(.putShort ~g!buffer ~datum)
                      :int `(.putInt ~g!buffer ~datum)
                      :long `(.putLong ~g!buffer ~datum)
                      :bytes `(let [datum# ~(with-meta datum {:tag "[B"})
                                    length# (alength datum#)]
                                (if (= 0 length#)
                                  (.putInt ~g!buffer (int -1))
                                  (do (.putInt ~g!buffer length#)
                                    (.put ~g!buffer datum#))))
                      :string `(let [inner# (.getBytes ~datum)
                                     length# (alength inner#)]
                                 (if (= 0 length#)
                                   (.putInt ~g!buffer (int -1))
                                   (do (.putInt ~g!buffer length#)
                                     (.put ~g!buffer inner#))))
                      :params* `(if ~datum
                                  (let [inner# (.getBytes ~datum)]
                                    (do (.putInt ~g!buffer (alength inner#))
                                      (.put ~g!buffer inner#)))
                                  (.putInt ~g!buffer 0))))
                  request)]
    `(let [~g!buffer (ByteBuffer/allocate (max (+ ~@lengths) ~(* 16 1024)))]
       ~@puts
       (.flip ~g!buffer))))

(defn ^:private ->read [type g!buffer]
  (case type
    :byte `(.get ~g!buffer)
    :char `(char (.get ~g!buffer))
    :short `(.getShort ~g!buffer)
    :int `(.getInt ~g!buffer)
    :long `(.getLong ~g!buffer)
    :bytes `(let [size# (.getInt ~g!buffer)]
              (when (not= -1 size#)
                (let [^"[B" data# (byte-array size#)]
                  (.get ~g!buffer data#)
                  data#)))
    :string `(let [size# (.getInt ~g!buffer)]
               (when (not= -1 size#)
                 (let [^"[B" data# (byte-array size#)]
                   (.get ~g!buffer data#)
                   (String. data#))))
    :record `(let [load-type# (.getShort ~g!buffer)]
               (cond (= -3 load-type#)
                     [(.getShort ~g!buffer) (.getLong ~g!buffer)]
                     
                     (= 0 load-type#)
                     (let [record-type# (char (.get ~g!buffer))
                           rid# (ORID. (.getShort ~g!buffer) (.getLong ~g!buffer))
                           version# (.getInt ~g!buffer)
                           content# (let [size# (.getInt ~g!buffer)]
                                      (when (not= -1 size#)
                                        (let [^"[B" data# (byte-array size#)]
                                          (.get ~g!buffer data#)
                                          data#)))]
                       (vary-meta (read-odoc (String. content#)) assoc
                                  :type (case record-type# \b ::bytes \d ::document)
                                  :rid rid#
                                  :version version#))))))

;; (defn ^:private ->response [buffer response]
;;   (let [g!buffer (with-meta (gensym "buffer") {:tag `ByteBuffer})]
;;     `(let [~g!buffer ~buffer]
;;        (if (-> ~g!buffer .get (= 0))
;;          [~@(map (comp #(->read % g!buffer) first)
;;                  response)]))))

(defmacro ^:private <=exceptions! [buffer]
  `(ex-info "There was an error communicating with OrientDB."
            {:type ::comm-error
             :exceptions (loop [all-pairs# []]
                           (if (= 1 (first (read-data* ~buffer [:byte])))
                             (let [[class# message#] (read-data* ~buffer [:string :string])]
                               (recur (conj all-pairs# {:class class#, :message message#})))
                             all-pairs#))}))

(defn get-char [^ByteBuffer buffer]
  (char (.get buffer)))

(defmulti read-type (fn [type buffer temp] type))

(do-template [<type> <size> <method>]
  (defmethod read-type <type> [_ ^ByteBuffer buffer temp]
    (let [rem (.remaining buffer)]
      (cond temp
            (let [{:keys [data rem-length]} temp
                  read-length (min rem-length rem)
                  read-offset (- <size> rem-length)]
              (.get buffer data read-offset read-length)
              (if (= rem-length read-length)
                (<method> (ByteBuffer/wrap data))
                {:data data :rem-length (- rem-length read-length)}))
            
            (>= rem <size>)
            (<method> buffer)

            :else
            (let [temp (byte-array <size>)]
              (.get buffer temp 0 rem)
              {:data temp :rem-length (- <size> rem)}))
      ))
  :byte  1 .get
  :char  1 get-char
  :short 2 .getShort
  :int   4 .getInt
  :long  8 .getLong)

(do-template [<type> <method>]
  (defmethod read-type <type> [_ ^ByteBuffer buffer temp]
    (if temp
      (let [{:keys [data size rem-length]} temp
            read-offset (- size rem-length)
            how-much (min rem-length (.remaining buffer))]
        (.get buffer data read-offset how-much)
        (if (= rem-length how-much)
          (<method> data)
          (assoc temp :rem-length (- rem-length how-much))))

      (let [size (.getInt buffer)
            rem (.remaining buffer)]
        (cond (= -1 size)
              nil

              (>= rem size)
              (let [holder (byte-array size)]
                (.get buffer holder)
                (<method> holder))

              :else
              (let [holder (byte-array size)]
                (.get buffer holder 0 rem)
                {:data holder :size size :rem-length (- size rem)})))
      ))
  :bytes  ->
  :string String.)

(defmethod read-type :record [_ ^ByteBuffer buffer temp]
  (let [rem (.remaining buffer)]
    (if temp
      (if (:done? temp)
        (let [{:keys [record-type rid version data]} temp]
          (vary-meta (-> data (or (byte-array)) String. read-odoc) assoc
                     :type (case record-type \b ::bytes \d ::document)
                     :rid rid
                     :version version))
        (let [return (read-type :bytes buffer (:data temp))]
          (if (map? return)
            (merge temp return)
            (recur :record buffer (merge temp {:data return :done? true})))))
      (let [load-type (.getShort buffer)]
        (case load-type
          -3 (ORID. (.getShort buffer) (.getLong buffer))
          0 (let [record-type (char (.get buffer))
                  rid (ORID. (.getShort buffer) (.getLong buffer))
                  version (.getInt buffer)
                  return (read-type :bytes buffer nil)]
              (if (map? return)
                (assoc return :rid rid :version version :record-type record-type)
                (recur :record buffer {:data return :done? true :rid rid :version version :record-type record-type}))
              )))
      )))

(defmacro ^:private read-data* [buffer types]
  (let [buffer (with-meta buffer {:tag `ByteBuffer})]
    `[~@(map #(->read % buffer) types)]))

(defmacro read-data [socket buffer out-chan types bindings & body]
  `(let [socket# ~socket
         buffer# ~(with-meta buffer {:tag `ByteBuffer})
         base-attachment# {:so-far [] :temp nil :reading-more? false}
         read?# (= 0 (.position buffer#))
         handler# (reify CompletionHandler
                    (~'completed [handler# bytes-read# attachment#]
                      (let [response-type# (when (or read?#
                                                     (:reading-more? attachment#))
                                             (.flip buffer#)
                                             (if read?#
                                               (let [response-type# (.get buffer#)
                                                     session-id# (.getInt buffer#)]
                                                 response-type#)
                                               0))]
                        (if (= 1 response-type#)
                          (let [exceptions# (<=exceptions! buffer#)]
                            (if (= "java.lang.NullPointerException" (get-in (ex-data exceptions#) [:exceptions 0 :class]))
                              (if (= 0 (.remaining buffer#))
                                (.read socket# (.clear buffer#) attachment# handler#)
                                (.completed handler# (.remaining buffer#) attachment#))
                              (return! ~out-chan exceptions#)))
                          (loop [attachment# attachment#
                                 types# (or (:types attachment#) ~types)
                                 so-far# (:so-far attachment#)]
                            (if types#
                              (let [ret# (read-type (first types#) buffer# (:temp attachment#))]
                                (if (and (map? ret#) (not (document? ret#)))
                                  (.read socket# (.clear buffer#) {:so-far so-far#, :temp ret# :reading-more? true :types types#} handler#)
                                  (recur nil
                                         (next types#)
                                         (conj so-far# ret#))))
                              (let [~bindings so-far#]
                                ~@body)
                              ))
                          )
                        ))
                    (~'failed [handler# err# _]
                      (return! ~out-chan err#)))]
     (if read?#
       (.read socket# buffer# base-attachment# handler#)
       (.completed handler# (.remaining buffer#) base-attachment#))
     ))

(def !request-id (atom 0))
(defmacro ^:private request->response [socket chan request response & body]
  (let [g!socket (gensym "socket")]
    `(let [~(with-meta g!socket {:tag `AsynchronousSocketChannel}) ~socket
           ~'*read-buffer* ~(->request request)
           req-id# (swap! !request-id inc)]
       (.write ~g!socket ~'*read-buffer* nil
               (->callback ~chan [self# bytes-written# _#]
                           (read-data ~g!socket (.clear ~'*read-buffer*) ~chan ~(vec (map first response))
                                      ~(vec (map second response))
                                      ~@body))
               ))
    ))

(defn ^:private read-clusters [socket buffer out-chan num-of-clusters callback]
  ((fn cycler [left so-far]
     (if (= 0 left)
       (callback so-far)
       (read-data socket buffer out-chan [:string :short :string :short]
                  [cluster-name cluster-id cluster-type cluster-dataSegmentId]
                  (cycler (dec left) (conj so-far [cluster-name cluster-id cluster-type cluster-dataSegmentId])))))
   num-of-clusters []))

(def ^:private +binary-commands+
  {DB_OPEN (fn [!out location ^String user-name ^String password]
             (let [[_ ^String host _ port* ^String db-name] (re-find #"remote:([\w\.]+)(:(\d+))?/(.+)" location)
                   port (if port* (Long/parseLong port*) 2424)
                   socket (AsynchronousSocketChannel/open)
                   ^ByteBuffer version-buff (ByteBuffer/allocate 2)]
               (doto socket
                 (.connect (InetSocketAddress. host port) nil
                           (->callback !out [self _ _]
                                       (.read socket version-buff nil
                                              (->callback !out [self bytes-read _]
                                                          (if (and (= 2 bytes-read)
                                                                   (-> ^ByteBuffer (.flip version-buff) .getShort +supported-protocol-versions+))
                                                            (request->response socket !out
                                                                               [[:byte DB_OPEN] [:int -1]
                                                                                [:string +driver-name+] [:string +driver-version+]
                                                                                [:short +protocol-version+] [:string ""]
                                                                                [:string db-name] [:string +db-type+]
                                                                                [:string user-name] [:string password]]
                                                                               [[:int session-id] [:short num-of-clusters]]
                                                                               (read-clusters socket *read-buffer* !out num-of-clusters
                                                                                              (fn [clusters]
                                                                                                (let [clusters (reduce (fn [total ^OCluster cluster]
                                                                                                                         (assoc total (.-name cluster) cluster))
                                                                                                                       {}
                                                                                                                       (for [[cluster-name cluster-id cluster-type cluster-dataSegmentId] clusters]
                                                                                                                         (OCluster. cluster-id cluster-name cluster-type cluster-dataSegmentId)))]
                                                                                                  (read-data socket *read-buffer* !out [:bytes :string]
                                                                                                             [cluster-config orientdb-release]
                                                                                                             (return! !out (DBConnection. socket session-id clusters cluster-config orientdb-release nil -1)))
                                                                                                  ))))
                                                            (async/put! !out (ex-info "The current binary protocol version is unsupported."
                                                                                      {:type ::unsupported-binary-protocol}))))))))))
   DB_SIZE (fn [^DBConnection db !out]
             (request->response (.-socket db) !out
                                [[:byte DB_SIZE] [:int (.-session-id db)]]
                                [[:long size]]
                                (return! !out size)))
   DB_COUNTRECORDS (fn [^DBConnection db !out]
                     (request->response (.-socket db) !out
                                        [[:byte DB_COUNTRECORDS] [:int (.-session-id db)]]
                                        [[:long size]]
                                        (return! !out size)))
   DB_CLOSE (fn [^DBConnection db !out]
              (let [^AsynchronousSocketChannel socket (.-socket db)]
                (.write socket (doto (ByteBuffer/allocate 5)
                                 (.put DB_CLOSE)
                                 (.putInt (.-session-id db))
                                 .flip)
                        nil
                        (reify CompletionHandler
                          (completed [self bytes-written _]
                            (.close socket)
                            (async/close! !out))
                          (failed [self err _]
                            (return! !out err))))))
   RECORD_LOAD (fn [^DBConnection db !out ^ORID rid ^String fetch-plan ignore-cache]
                 (request->response (.-socket db) !out
                                    [[:byte RECORD_LOAD] [:int (.-session-id db)]
                                     [:short (.cluster-id rid)] [:long (.cluster-position rid)]
                                     [:string (or fetch-plan "")] [:byte (byte (if ignore-cache 1 0))]
                                     [:byte (byte 0)]]
                                    [[:byte payload-status]]
                                    (if (= 1 payload-status)
                                      (read-data (.-socket db) *read-buffer* !out [:bytes :int :char]
                                                 [^"[B" record-content record-version record-type]
                                                 (return! !out (case record-type
                                                                 \d (vary-meta (read-odoc (String. record-content)) merge
                                                                               {:rid rid
                                                                                :version record-version})
                                                                 \b (vary-meta (blob record-content) merge
                                                                               {:rid rid
                                                                                :version record-version}))))
                                      (return! !out (ex-info "The requested record was not found."
                                                             {:type ::record-not-found :rid rid})))))
   RECORD_CREATE (fn [^DBConnection db !out record cluster-to-save]
                   (let [^OCluster cluster (get (.clusters db) (or cluster-to-save (-> record meta :class) "default"))
                         [record-type record-content] (case (type record)
                                                        ::document [\d (.getBytes (write-odoc record))]
                                                        ::bytes [\b (:body record)])]
                     (request->response (.-socket db) !out
                                        [[:byte RECORD_CREATE] [:int (.-session-id db)]
                                         [:int (.-data-segment-id cluster)] [:short (.-id cluster)]
                                         [:bytes record-content]
                                         [:byte (byte record-type)] [:byte (byte 0)]]
                                        [[:long cluster-position] [:int record-version]]
                                        (return! !out (vary-meta record merge
                                                                 {:rid (ORID. (.-id cluster) cluster-position)
                                                                  :version record-version})))
                     ))
   RECORD_UPDATE (fn [^DBConnection db !out record]
                   (let [{:keys [^ORID rid version]} (meta record)
                         [record-type record-content] (case (type record)
                                                        ::document [\d (.getBytes (write-odoc record))]
                                                        ::bytes [\b (:body record)])]
                     (request->response (.-socket db) !out
                                        [[:byte RECORD_UPDATE] [:int (.-session-id db)]
                                         [:short (.cluster-id rid)] [:long (.cluster-position rid)]
                                         [:bytes record-content]
                                         [:int version] [:byte (byte record-type)]
                                         [:byte (byte 0)]]
                                        [[:int record-version]]
                                        (return! !out (vary-meta record merge
                                                                 {:version record-version})))))
   RECORD_DELETE (fn [^DBConnection db !out odoc]
                   (let [{:keys [^ORID rid version]} (meta odoc)]
                     (request->response (.-socket db) !out
                                        [[:byte RECORD_DELETE] [:int (.-session-id db)]
                                         [:short (.cluster-id rid)] [:long (.cluster-position rid)]
                                         [:int version] [:byte (byte 0)]]
                                        [[:byte payload-status]]
                                        (return! !out (= 1 payload-status)))))

   COMMAND (let [synch-mode (byte \s)
                 collect-list! (fn collect-list! [socket buffer out-chan remaining records]
                                 (if (= 0 remaining)
                                   (return! out-chan records)
                                   (read-data socket buffer out-chan [:record]
                                              [rec]
                                              (collect-list! socket buffer out-chan (dec remaining) (conj records rec)))))
                 process-response (fn [socket buffer out-chan synch-result-type]
                                    (case synch-result-type
                                      \n nil
                                      \r (read-data socket buffer out-chan [:record]
                                                    [record]
                                                    (return! out-chan record))
                                      \l (read-data socket buffer out-chan [:int] [remaining]
                                                    (collect-list! socket buffer out-chan remaining []))
                                      \a (read-data socket buffer out-chan [:string]
                                                    [ret]
                                                    (return! out-chan (-> ret read-value second))))
                                    )]
             (fn [^DBConnection db !out ^String class-name ^String text params ^String fetch-plan ^String language]
               (case class-name
                 "q" (let [fetch-plan (or fetch-plan "")
                           params (prepare-sql-params params)]
                       (request->response (.-socket db) !out
                                          [[:byte COMMAND] [:int (.-session-id db)]
                                           [:byte synch-mode]
                                           [:int (+ 4 1
                                                    4 (alength (.getBytes text))
                                                    4
                                                    4 (alength (.getBytes fetch-plan))
                                                    4 (if params (alength (.getBytes params)) 0))]
                                           [:string class-name]
                                           [:string text]
                                           [:int (int -1)]
                                           [:string fetch-plan]
                                           [:params* params]]
                                          [[:byte synch-result-type]]
                                          (process-response (.-socket db) *read-buffer* !out (char synch-result-type))))
                 "c" (let [params (prepare-sql-params params)]
                       (request->response (.-socket db) !out
                                          [[:byte COMMAND] [:int (.-session-id db)]
                                           [:byte synch-mode]
                                           [:int (+ 4 1
                                                    4 (alength (.getBytes text))
                                                    1
                                                    4 (if params (alength (.getBytes params)) 0)
                                                    1)]
                                           [:string class-name]
                                           [:string text]
                                           [:byte (byte (if params 1 0))]
                                           [:params* params]
                                           [:byte (byte 0)]]
                                          [[:byte synch-result-type]]
                                          (process-response (.-socket db) *read-buffer* !out (char synch-result-type))))
                 "s" (request->response (.-socket db) !out
                                        [[:byte COMMAND] [:int (.-session-id db)]
                                         [:byte synch-mode]
                                         [:int (+ 4 1
                                                  4 (alength (.getBytes language))
                                                  4 (alength (.getBytes text))
                                                  4
                                                  4)]
                                         [:string class-name]
                                         [:string language]
                                         [:string text]
                                         [:int (int -1)] [:int (int 0)]]
                                        [[:byte synch-result-type]]
                                        (process-response (.-socket db) *read-buffer* !out (char synch-result-type))))))
   TX_COMMIT (fn [^DBConnection db !out use-transaction-log?]
               (let [^OOptimisticTx tx (._tx db)
                     tx-payload (.payload tx)
                     ^AsynchronousSocketChannel socket (.-socket db)
                     created-odocs (:created tx-payload)
                     updated-odocs (:updated tx-payload)
                     [_created-size _created] (reduce (fn [[size data] datum]
                                                        (let [{:keys [^ORID rid type]} (meta datum)
                                                              [record-type ^"[B" record-content] (case type
                                                                                                   ::document [\d (.getBytes (write-odoc datum))]
                                                                                                   ::bytes [\b (:body datum)])
                                                              content-size (alength record-content)]
                                                          [(+ size 1 2 8 1 4 content-size)
                                                           (conj data [(byte 3) (.cluster-id rid) (.cluster-position rid) (byte record-type) (int content-size) record-content])]))
                                                      [0 '()] (vals created-odocs))
                     [_updated-size _updated] (reduce (fn [[size data] datum]
                                                        (let [{:keys [^ORID rid version]} (meta datum)
                                                              [record-type ^"[B" record-content] (case type
                                                                                                   ::document [\d (.getBytes (write-odoc datum))]
                                                                                                   ::bytes [\b (:body datum)])
                                                              content-size (alength record-content)]
                                                          [(+ size 1 2 8 1 4 4 content-size)
                                                           (conj data [(byte 1) (.cluster-id rid) (.cluster-position rid) (byte record-type) version (int content-size) record-content])]))
                                                      [0 '()] (vals updated-odocs))
                     [_deleted-size _deleted] (reduce (fn [[size data] datum]
                                                        (let [{:keys [^ORID rid type version]} (meta datum)]
                                                          [(+ size 1 2 8 1 4)
                                                           (conj data [(byte 2) (.cluster-id rid) (.cluster-position rid) (byte (case type ::document \d ::bytes \b)) version])]))
                                                      [0 '()] (vals (:deleted tx-payload)))
                     total-size (+ 1 4 4 1 (count _created) _created-size (count _updated) _updated-size (count _deleted) _deleted-size 1 4)
                     buffer (ByteBuffer/allocate total-size)]
                 (doto buffer
                   (.put TX_COMMIT)
                   (.putInt (.-session-id db))
                   (.putInt (.-id tx))
                   (.put (byte (if use-transaction-log? 1 0))))
                 (doseq [[^Byte op cluster-id cluster-position ^Byte record-type content-size ^"[B" content] _created]
                   (doto buffer
                     (.put (byte 1))
                     (.put op)
                     (.putShort cluster-id)
                     (.putLong cluster-position)
                     (.put record-type)
                     (.putInt content-size)
                     (.put content)))
                 (doseq [[^Byte op cluster-id cluster-position ^Byte record-type version content-size ^"[B" content] _updated]
                   (doto buffer
                     (.put (byte 1))
                     (.put op)
                     (.putShort cluster-id)
                     (.putLong cluster-position)
                     (.put record-type)
                     (.putInt version)
                     (.putInt content-size)
                     (.put content)))
                 (doseq [[^Byte op cluster-id cluster-position ^Byte record-type version] _deleted]
                   (doto buffer
                     (.put (byte 1))
                     (.put op)
                     (.putShort cluster-id)
                     (.putLong cluster-position)
                     (.put record-type)
                     (.putInt version)))
                 (doto buffer
                   (.put (byte 0))
                   (.putInt -1)
                   .flip)
                 (.write socket buffer nil
                         (reify CompletionHandler
                           (completed [self bytes-written _]
                             (.clear buffer)
                             (.read socket buffer nil
                                    (reify CompletionHandler
                                      (completed [self bytes-read _]
                                        (.flip buffer)
                                        (if (= 0 (.get buffer))
                                          (do (.getInt buffer) ;; Session ID
                                            (let [created-record-count (.getInt buffer)]
                                              (dotimes [_ created-record-count]
                                                (let [temp (ORID. (.getShort buffer) (.getLong buffer))
                                                      ^ORID set-id (-> created-odocs (get temp) meta :rid)]
                                                  (doto set-id
                                                    (.cluster-id (.getShort buffer))
                                                    (.cluster-position (.getLong buffer))))))
                                            ;; (let [updated-record-count (.getInt buffer)]
                                            ;;   (dotimes [_ updated-record-count]
                                            ;;     (let [temp (ORID. (.getShort buffer) (.getLong buffer))
                                            ;;           new-version (.getInt buffer)]
                                            ;;       )))
                                            (return! !out true))
                                          (do (.getInt buffer) ;; Session ID
                                            (return! !out (<=exceptions! buffer)))))
                                      (failed [self err _]
                                        (return! !out err)))))
                           (failed [self err _]
                             (return! !out err))))))
   })

(defn ^:private execute-binary-command! [command & args]
  (apply (get +binary-commands+ command) args))

;; [Interface]
(defn cluster-name->id [db name]
  (-> db .-clusters ^OCluster (get name) .-id))

(defn cluster-id->name [db id]
  (if-let [^OCluster cluster (some #(if (= id (.-id ^OCluster %))
                                      %)
                                   (vals (.-clusters db)))]
    (.-name cluster)))

(defn id
  ([^DBConnection db ^String cluster-name ^Long cluster-position]
     (ORID. (cluster-name->id db cluster-name) (long cluster-position)))
  ([^Short cluster-id ^Long cluster-position]
     (ORID. (short cluster-id) (long cluster-position))))

(defn id? [x]
  (instance? ORID x))

(defn size [db]
  (let [!out (async/chan)]
    (execute-binary-command! DB_SIZE db !out)
    !out))

(defn count-records [db]
  (let [!out (async/chan)]
    (execute-binary-command! DB_COUNTRECORDS db !out)
    !out))

(defn open! [{:keys [location user-name password]}]
  (let [!out (async/chan)]
    (execute-binary-command! DB_OPEN !out location user-name password)
    !out))

(defn close! [db]
  (let [!out (async/chan)]
    (execute-binary-command! DB_CLOSE db !out)
    !out))

(defn load! [^DBConnection db rid & [fetch-plan ignore-cache]]
  (let [!out (async/chan)]
    (if-let [^OOptimisticTx tx (._tx db)]
      (let [payload (.payload tx)]
        (cond-let [updated (get-in payload [:updated rid])]
                  (return! !out updated)

                  [deleted (get-in payload [:deleted rid])]
                  (async/close! !out)
                  
                  (execute-binary-command! RECORD_LOAD db !out rid fetch-plan ignore-cache)))
      (execute-binary-command! RECORD_LOAD db !out rid fetch-plan ignore-cache))
    !out))

(defn save! [^DBConnection db odoc & [cluster-to-save]]
  (let [!out (async/chan)]
    (if-let [^OOptimisticTx tx (._tx db)]
      (if-let [rid (-> odoc meta :rid)]
        (let [payload (.payload tx)
              [payload* return] (cond (get-in payload [:deleted rid])
                                      [payload nil]

                                      (get-in payload [:created rid])
                                      [(assoc-in payload [:created rid] odoc) odoc]

                                      (get-in payload [:updated rid])
                                      [(assoc-in payload [:updated rid] odoc) odoc]
                                      
                                      :else
                                      (let [odoc* (vary-meta odoc update-in [:version] inc)]
                                        [(assoc-in payload [:updated rid] odoc*) odoc*]))]
          (.payload tx payload*)
          (return! !out return))
        (let [tid (temp-id! tx)
              odoc* (vary-meta odoc assoc :rid tid)]
          (.payload tx (assoc-in (.payload tx) [:created tid] odoc*))
          (return! !out odoc*)))
      (if-let [rid (-> odoc meta :rid)]
        (execute-binary-command! RECORD_UPDATE db !out odoc)
        (execute-binary-command! RECORD_CREATE db !out odoc cluster-to-save)))
    !out))

(defn document [class body]
  {:pre [(map? body)]}
  (with-meta body {:type ::document :class class}))

(defn blob [^"[B" bytes]
  (with-meta {:body bytes :size (alength bytes)}
    {:type ::bytes}))

(do-template [<name> <type>]
  (defn <name> [orecord]
    (= <type> (type orecord)))

  document? ::document
  blob? ::bytes)

(let [delete!* (fn [^DBConnection db !out odoc]
                 (let [rid (-> odoc meta :rid)]
                   (if-let [^OOptimisticTx tx (._tx db)]
                     (let [payload (.payload tx)
                           remover #(if (get % rid) (dissoc % rid) %)
                           payload* (-> payload
                                        (update-in [:deleted] #(if (temp-id? rid) % (assoc % rid odoc)))
                                        (update-in [:created] remover)
                                        (update-in [:updated] remover))]
                       (.payload tx payload*)
                       (return! !out true))
                     (execute-binary-command! RECORD_DELETE db !out odoc))))]
  (defn delete! [db odoc|orid]
    (let [!out (async/chan)]
      (if (id? odoc|orid)
        (go (delete!* db !out (<! (load! db odoc|orid))))
        (delete!* db !out odoc|orid))
      !out)))

;; Transactions
(defn begin-tx! [^DBConnection db]
  (doto db
    (._tx (OOptimisticTx. (inc-tx-counter! db) (long -1) {:created {} :updated {} :deleted {}}))))

(defn rollback-tx! [^DBConnection db]
  (doto db
    (._tx nil)))

(defn commit-tx! [^DBConnection db & [use-transaction-log?]]
  (let [!out (async/chan)]
    (execute-binary-command! TX_COMMIT db !out use-transaction-log?)
    !out))

;; Commands
(defn query [^DBConnection db ^String text & [args fetch-plan]]
  (let [!out (async/chan)]
    (execute-binary-command! COMMAND db !out "q" text args fetch-plan nil)
    !out))

(defn execute! [^DBConnection db ^String text & [args]]
  (let [!out (async/chan)]
    (execute-binary-command! COMMAND db !out "c" text args nil nil)
    !out))

(defn eval! [^DBConnection db ^String language ^String text]
  (let [!out (async/chan)]
    (execute-binary-command! COMMAND db !out "s" text nil nil language)
    !out))

;; Connection pools
(defn acquire-db! [^ConnectionPool pool]
  (let [!out (async/chan)]
    (async/put! (.-acquire-chan pool) !out)
    !out))

(defn release-db! [^ConnectionPool pool db]
  (async/put! (.-release-chan pool) db)
  nil)

(defn close-pool! [^ConnectionPool pool]
  (async/put! (.-close-chan pool) true)
  nil)

(let [return! (fn [channel val]
                (doto channel
                  (async/put! val)
                  (async/close!)))]
  (defn pool [config max-conns]
    {:pre [(or (> max-conns 0)
               (= -1 max-conns))]}
    ;; Generate the pool object.
    (let [<acquire> (async/chan)
          <release> (async/chan)
          <close> (async/chan)
          pool (ConnectionPool. <acquire> <release> <close> config max-conns)]
      ;; Handle requests.
      (go (let [[opened-conns released-conns] (loop [opened-conns 0
                                                     conns #{}]
                                                (alt! <acquire> ([conn] (cond (not (empty? conns))
                                                                              (let [db (first conns)]
                                                                                (return! conn db)
                                                                                (recur opened-conns (disj conns db)))
                                                                              
                                                                              (or (< opened-conns max-conns)
                                                                                  (= -1 max-conns))
                                                                              (let [db (<! (open! config))]
                                                                                (return! conn db)
                                                                                (recur (inc opened-conns) (disj conns db)))

                                                                              :else
                                                                              (let [db (<! <release>)]
                                                                                (return! conn db)
                                                                                (recur opened-conns conns))))
                                                      <release> ([return] (recur opened-conns (conj conns return)))
                                                      <close> ([_] [opened-conns conns])))]
            ;; Clean-up already-released connections.
            (dorun close! released-conns)
            ;; Deny new acquisition requests and release pending connections.
            (loop [remaining (- opened-conns (count released-conns))]
              (when (> remaining 0)
                (alt! <acquire> ([conn] (return! conn (ex-info "The pool is closed." {:type ::connection-pool-closed}))
                                   (recur remaining))
                      <release> ([return] (close! return)
                                   (recur (dec remaining))))))
            ))
      ;; Return the pool.
      pool)))

;; [Syntax]
(defmacro tx [db & body]
  `(try (begin-tx! ~db)
     (let [res# (do ~@body)]
       (<! (commit-tx! ~db true))
       res#)
     (catch Exception e#
       (rollback-tx! ~db)
       (throw e#))))

(defmacro with-db [[db conf] & body]
  {:pre [(symbol? db)]}
  `(let [~db (<! (open! ~conf))]
     (try (do ~@body)
       (finally (<! (close! ~db))))))

(defmacro with-pooled-db [[db pool] & body]
  {:pre [(symbol? db)]}
  `(let [~db (<! (acquire-db! ~pool))]
     (try (do ~@body)
       (finally (release-db! ~pool ~db)))))
