(ns etl.olt-huawei
  (:require
   [etl.telnet-agent :as agent]
   [etl.helper :refer [stateof conf]]
   [etl.olt :refer [login-en logout get-state]]
   [clojure.tools.logging :as log]
   [clojure.string :as str]
   [etl.parse-huawei :as parser]))

(defn login
  [ip]
  (login-en ip (:hw-login conf) (:hw-pass conf)))

(defn card-info
  "Get output of command 'display board 0' for a given olt, and parse the output
  to form a vector of card map which is ready to insert to database"
  [olt]
  (if-let [client (login (:ip olt))]
    (let [card-out (agent/cmd client "display board 0")]
      (logout client)
      (map #(merge {:olt_id (:id olt)} %)
           (parser/card-list card-out)))))

(def extract-onu-sec
  "x-form to extract onu items from bulk command output"
  (comp
   (map str/trim)
   (filter #(re-find #"^0/" %))
   (map #(str/split % #"\s+"))))

(def extract-optical-sec
  "x-form to extract optical info from command output"
  (comp
   (map str/trim)
   (filter #(re-find #"^\d+" %))
   (map #(str/split % #"\s+"))))

(defn sn-map
  "transform sn items to sn map"
  [items]
  {:pon (get items 1)
   :oid (read-string (get items 2))
   :sn (get items 3)
   :type "HW-XXX"
   :auth "sn"
   :model "gpon"})

(def extract-sn-map
  "x-form to extract onu sn map"
  (comp
   (filter #(= 4 (count %)))
   (map sn-map)))

(defn state-map
  "transform state items to state map"
  [items]
  {:pon (get items 1)
   :oid (read-string (get items 2))
   :state (stateof (get items 5))})

(def extract-state-map
  "x-form to extract onu state map"
  (comp
   (filter #(= 9 (count %)))
   (map state-map)))

(defn rx-power-map
  "transform optical info items to rx-power map"
  [pon items]
  (let [rx (read-string (get items 1))]
    {:pon pon
     :oid (read-string (get items 0))
     :rx_power (if (number? rx) rx -404)}))
   
(defn board-out
  "Send 'display board 0/$card-no' command and get onu sn/state info"
  [client icard sn?]
  (let [cmd-out (agent/cmd client (format "display board 0/%d" icard))
        lines (str/split-lines cmd-out)
        onu-items (transduce extract-onu-sec conj [] lines)]
    (if sn?
      (transduce extract-sn-map conj [] onu-items)
      (transduce extract-state-map conj [] onu-items))))

(defn port-state-out
  "Send 'display ont info 0 slot port all' command to get state info
   for a given card/port"
  [client icard iport]
  (let [cmd-out (agent/cmd client (format "display ont info 0 %s %s all"
                                          icard iport))
        lines (str/split-lines cmd-out)
        onu-items (transduce extract-onu-sec conj [] lines)]
    (transduce extract-state-map conj [] onu-items)))

(defn olt-onu-info
  [olt cards sn?]
  (if-let [s (login (:ip olt))]
    (try
      (flatten (doall (for [c cards]
                        (map #(merge % {:olt_id (:id olt)}) (board-out s c sn?)))))
      (catch Exception ex
        (println (str "caught exception: " (.getMessage ex)))
        (log/error (format "caught exception in olt-onu-info for olt [%s][%s]: %s"
                           (:name olt) (:ip olt) (.getMessage ex)))
        nil)
      (finally (logout s)))))

(defn optical-info-out
  "Send 'display ont optical-info 0 all' command and get onu rx-power info"
  [client icard iport]
  (let [s-new (agent/hw-config client icard)
        cmd-out (agent/cmd s-new (format "display ont optical-info %d all" iport))
        lines (str/split-lines cmd-out)
        onu-items (transduce extract-optical-sec conj [] lines)
        pon (format "%d/%d" icard iport)]
    ;;;(println "optical-info-out:" pon) 
    (map #(rx-power-map pon %) onu-items)))

(defn olt-onu-rx-power
  [olt cards]
  (if-let [s (login (:ip olt))]
    (try
      (flatten (doall (for [c cards]
                        (doall (for [p (range 0 16)]
                                 (map #(merge % {:olt_id (:id olt)})
                                      (optical-info-out s c p)))))))
      (catch Exception ex
        (println (str "caught exception: " (.getMessage ex)))
        (log/error (format "caught exception in olt-onu-info for olt [%s][%s]: %s"
                           (:name olt) (:ip olt) (.getMessage ex)))
        nil)
      (finally (logout s)))))

(defn merge-the-onu
  "find the onu from rx-power vector and merge info"
  [rxs state]
  (let [rx
        (first
         (filter #(and
                   (= (:olt_id state) (:olt_id %))
                   (= (:pon state) (:pon %))
                   (= (:oid state) (:oid %)))
                 rxs))]
    (merge {:in_bps 0 :out_bps 0 :in_bw 0 :out_bw 0}
           state
           (or rx {:rx_power -100}))))
  
(defn olt-onu-all
  [olt cards]
  (let [states (olt-onu-info olt cards false)
        rxs (olt-onu-rx-power olt cards)]
    (map #(merge-the-onu rxs %) states)))

(defn states-for-onus
  "Get latest states for given onus"
  [client onus]
  (let [pons (distinct (map :pon onus))
        states (flatten
                (doall (for [pon pons]
                         (let [[icard iport]
                               (map read-string (str/split pon #"/"))]
                           (port-state-out client icard iport)))))
        rxs (flatten
             (doall (for [pon pons]
                      (let [[icard iport]
                            (map read-string (str/split pon #"/"))]
                        (optical-info-out client icard iport)))))
        all (map #(merge-the-onu rxs %) states)]
    (map #(merge {:onu_id (:id %)} (get-state all (:pon %) (:oid %))) onus)))

(defn latest-states
  "Get latest state maps for given olt and onus"
  [olt onus]
  (log/info (format "Processing latest-states on [%s][%s], onu count: [%d]"
                    (:name olt) (:ip olt) (count onus)))
  (if-let [s (login (:ip olt))]
    (try
      (states-for-onus s onus)
      (catch Exception ex
        (println (format "in latest-states caught exception: %s" (.getMessage ex)))
        (log/error (format "caught exception in olt-sn for olt [%s][%s]: %s"
                           (:name olt) (:ip olt)  (.getMessage ex))))
      (finally (logout s)))))

(defn mytest []
  (let [olt {:id 111 :ip "10.250.79.141" :name "test-olt"}
        cards [1 2]]
    (olt-onu-all olt cards)))
  ;;(olt-onu-info {:id 111 :ip "10.250.79.141" :name "test-olt"} [1 2] false))
  ;;(config-mode {:id 111 :ip "10.250.79.141" :name "test-olt"}))
