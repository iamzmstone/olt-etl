(ns etl.parse
  (:require
   [etl.helper :refer [stateof]]
   [clojure.string :as str]))

(defn gbk-str
  "decode gbk encoding string"
  [s]
  (when s
    (String. (.getBytes s "iso8859-1") "gbk")))

;;; functions for parsing card part
(defn- is-uplink?
  [type]
  (#{"GUFQ" "HUTQ" "HUVQ"} type))

(defn card-model
  "get gpon epon model from card-type"
  [type]
  (cond
    (re-find #"GTG" type) "GPON"
    (re-find #"ETG" type) "EPON"
    (re-find #"ETTO" type) "EPON"
    (is-uplink? type) "UPLINK"
    :else nil))

(defn parse-card
  "parse one line of card info"
  [line]
  (let [cols (str/split line #"\s+")]
    (if (= 9 (count cols))
      {:slot (read-string (get cols 2))
       :card_type (get cols 4)
       :port_cnt (read-string (if (= "N/A" (get cols 5)) "0" (get cols 5)))
       :hard_ver (get cols 6)
       :soft_ver (get cols 7)
       :status (get cols 8)
       :model (card-model (get cols 4))}
      nil)))

(defn card-list
  "Get a list of card map from output of show card"
  [card-section]
  (remove nil?
          (reduce conj [] (map parse-card
                               (filter #(re-find #"^\d" %)
                                       (str/split-lines card-section))))))

;;; functions for parsing onu state
(defn parse-status
  "parse onu state line and save to a map"
  [line]
  (let [cols (str/split line #"\s+")
        onu-str (get cols 0)
        state (cond
                (re-find #"^gpon" onu-str) {:m "gpon" :s (get cols 4)}
                (re-find #"^epon" onu-str) {:m "epon" :s (get cols 2)}
                :else {:m "gpon" :s (get cols 3)})
        m (re-find #"(\d+\/\d+):(\d+)" onu-str)]
    (when m
      {:pon (get m 1) :oid (read-string (get m 2))
       :model (:m state) :state (stateof (:s state))})))

(defn onu-state-list
  "Get a list of state map from output of onu-status"
  [state-section]
  (reduce conj []
          (map parse-status
               (filter #(re-find #"\d+\/\d+:\d+" %)
                       (str/split-lines state-section)))))

;;; functions for parsing loid/sn/mac of onu
(defn- pon-intf
  [list]
  (if-let [[_ m intf] (re-find #"([g|e]pon)-olt_1/(\d+\/\d+)" (nth list 0))]
    {:pon intf :model m}))

(defn- pon-intf-line
  [line]
  (if-let [[_ m intf] (re-find #"([g|e]pon)-olt_1/(\d+\/\d+)" line)]
    {:pon intf :model m}))

(defn onu-sn
  [str]
  (if-let [[_ oid type auth sn] (re-find #"onu (\d+) type (\S+) (sn|loid|mac) (\S+)" str)]
    {:oid oid :type type :auth auth :sn sn}))

(defn- onu-sns
  [list]
  (map onu-sn list))

(defn- combine-sn
  [intf onus]
  (map #(merge intf %) onus))

(defn- remove-intf-without-onu
  [sn-lines]
  (reduce #(if (and (not (nil? (last %1)))
                    (re-find #"^interface" (last %1))
                    (re-find #"^interface" %2))
             (assoc %1 (dec (count %1)) %2)
             (conj %1 %2)) [] sn-lines))

(defn sn-list
  "Get a list of sn map from sn cmds output string"
  [sn-cmd-out]
  (let [sn-lines (filter #(re-find #"(^interface|onu \d+ type )" %)
                         (str/split-lines sn-cmd-out))
        part-sn (remove-intf-without-onu sn-lines)
        sns (partition-by #(re-find #"^interface" %) part-sn)
        intfs (map pon-intf (take-nth 2 sns))
        onus (map onu-sns (take-nth 2 (rest sns)))]
    (flatten (reduce conj [] (map combine-sn intfs onus)))))

(defn- desc-info
  [list]
  {:name (gbk-str (last (re-find #"(name|description) (.*)" (first list))))})

(defn pon-desc-list
  "Get a list of pon description map from sn cmds output string"
  [sn-cmd-out]
  (let [desc-lines (filter #(re-find #"(^interface|^\s*description|^\s*name)" %)
                         (str/split-lines sn-cmd-out))
        part-desc (partition-by #(re-find #"^interface" %)
                                (remove-intf-without-onu desc-lines))
        intfs (map pon-intf (take-nth 2 part-desc))
        descs (map desc-info (take-nth 2 (rest part-desc)))]
    (reduce conj [] (map merge intfs descs))))

(defn- onu-intf
  [list]
  (if-let [[_ pon oid] (re-find #"show int [g|e]pon-onu_1/(\d+\/\d+):(\d+)"
                                (nth list 0))]
    {:pon pon :oid oid}))

(defn- traffic-line
  [line]
  (if-let [[_ in] (re-find #"Input rate :\s+(\d+) Bps" line)]
    {:in_bps (read-string in)}
    (if-let [[_ out] (re-find #"Output rate:\s+(\d+) Bps" line)]
      {:out_bps (read-string out)}
      (if-let [[_ _ in-bw] (re-find #"Input bandwidth (throughput|thoughput) :(\S+)" line)]
        {:in_bw (if (= "N/A" in-bw) 0 (read-string in-bw))}
        (if-let [[_ _ out-bw] (re-find #"Output bandwidth (throughput|thoughput):\s*(\S+)" line)]
          {:out_bw (if (= "N/A" out-bw) 0 (read-string out-bw))})))))

(defn traffic-map
  [list]
  (reduce merge {:in_bps 0 :out_bps 0 :in_bw 0 :out_bw 0}
          (map traffic-line list)))

(defn rx-map
  "Form a onu rx power map from parsing onu-rx line"
  [list]
  (let [line (first (filter #(re-find #"^[g|e]pon-onu_1" %) list))
        [onu rx] (str/split line #"\s+")]
    (if-let [[_ pon oid] (re-find #"(\d+\/\d+):(\d+)" onu)]
      {:pon pon :oid oid :rx_power (if (= rx "N/A") "-100" (read-string rx))})))

;;; property description $$%s$$
;;; name %s
(defn onu-name
  "Parse config of onu and extract name/description of onu"
  [onu-cfg-section model]
  (if-let [name-line
        (first (filter #(re-find #"^\s*(property description|name)" %)
                       (str/split-lines onu-cfg-section)))]
    (cond
      (= model "epon") (second (re-find #"\$\d*\$(.+)(\$\$|$)" name-line))
      (= model "gpon") (second (re-find #"name (.+)" name-line)))
    "No-Name"))

(defn uplink-state
  [cmd-out]
  (second
   (re-find #"line protocol is (\w+)," cmd-out)))

(defn uplink-rx-power
  [cmd-out]
  (let [s-rx (second (re-find #"(?m)RxPower\s+:\s+([\d-\.]+)\s*\(dbm\)" cmd-out))]
    (if s-rx
      (let [rx (read-string s-rx)]
        (if (number? rx)
          rx
          -404))
      -100)))
