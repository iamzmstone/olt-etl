(ns etl.olt-fh
  (:require
   [etl.telnet-agent :as agent]
   [etl.helper :refer [stateof conf]]
   [etl.olt :refer [login-en logout get-state]]
   [clojure.tools.logging :as log]
   [clojure.string :as str]
   [etl.parse-fh :as parser]))

(def olt-login (:fh-login conf))
(def olt-pass (:fh-pass conf))

(defn login
  [ip]
  (login-en ip olt-login olt-pass))

(defn card-info
  "Get output of command 'show version' for a given olt, and parse the output
  to form a vector of card map which is ready to insert to database"
  [olt]
  (log/info (format "card-info for olt [%s:%s]" (:name olt) (:ip olt)))
  (let [client (login (:ip olt))]
    (when client
      (let [card-out (agent/cmd client "show version")]
        (logout client)
        (map #(merge {:olt_id (:id olt)} %)
             (parser/card-list card-out))))))

(def sn-fmt "---- --- --- -------------- -- --- --- ------------ ---------- ------------------------ ------------")

(defn fld-pos
  "reduce list of start pos of each field"
  []
  (reduce #(conj %1 (+ (last %1) (inc %2)))
          [0] (map count (str/split sn-fmt #" "))))

(defn split-sn-flds
  "split fields of sn line according to fld-pos"
  [sn-line]
  (let [pos (fld-pos)]
    (mapv #(str/trim (subs sn-line %1 %2))
         pos (rest pos))))
       
(def extract-onu-sec
  "x-form to extract onu items from bulk command output"
  (comp
   (filter #(re-find #"^\d+\s+\d+" %))
   (map #(split-sn-flds %))))

(defn sn-map
  "transform sn items to sn map"
  [items]
  {:pon (format "%s/%s" (get items 0) (get items 1))
   :oid (read-string (get items 2))
   :sn (get items 9)
   :type (get items 3)
   :auth "loid"
   :model "gpon"})

(defn sn-cmd-out
  [client slot pon]
  (let [sn-sec (agent/cmd client
                          (format "show authorization slot %d pon %d" slot pon))
        sn-flds (transduce extract-onu-sec conj [] (str/split-lines sn-sec))]
    (map sn-map sn-flds)))

(defn olt-sn
  [olt cards]
  (if-let [s (login (:ip olt))]
    (let [s-new (agent/fh-onu s)]
      (try
        (flatten
         (doall (for [c cards]
                  (doall (for [i (range 1 17)]
                           (map #(merge % {:olt-id (:id olt)})
                                (sn-cmd-out s-new c i)))))))
        (catch Exception ex
          (println (str "caught exception: " (.getMessage ex)))
          (log/error (format "caught exception in olt-sn for olt [%s][%s]: %s"
                             (:name olt) (:ip olt) (.getMessage ex)))
          nil)
        (finally (logout s))))))

(def extract-state-sec
  "x-form to extract onu state items from command output"
  (comp
   (filter #(re-find #"^SLOT" %))
   (map #(str/split % #"\s+"))))

(defn state-map
  "transform state items to state-map"
  [items]
  {:pon (format "%s/%s" (get items 1) (get items 3))
   :oid (read-string (get items 5))
   :state (stateof (get items 8))})

(defn state-cmd-out
  [client slot pon]
  (let [sn-sec (agent/cmd client
                          (format "show onu_state slot %d pon %d" slot pon))
        sn-flds (transduce extract-state-sec conj [] (str/split-lines sn-sec))]
    (map state-map sn-flds)))

(defn olt-onu-info
  [olt cards sn?]
  (if-let [s (login (:ip olt))]
    (let [s-new (agent/fh-onu s)]
      (try
        (flatten
         (doall (for [c cards]
                  (doall (for [i (range 1 17)]
                           (map #(merge % {:olt_id (:id olt)})
                                (if sn?
                                  (sn-cmd-out s-new c i)
                                  (state-cmd-out s-new c i))))))))
        (catch Exception ex
          (println (str "caught exception: " (.getMessage ex)))
          (log/error (format "caught exception in olt-onu-info for olt [%s][%s]: %s"
                             (:name olt) (:ip olt) (.getMessage ex)))
          nil)
        (finally (logout s))))))

(defn optical-cmd-out
  [client state]
  (let [[slot pon] (str/split (:pon state) #"/")
        rx-sec (agent/cmd
                client
                (format "show optic_module slot %s pon %s onu %d"
                        slot pon (:oid state)))]
    (let [m (parser/parse-rx-power rx-sec)]
      (if-not m
        (println "Can't get rx power for state:" state))
      m)))

(defn olt-onu-rx-merge
  [olt states]
  (if-let [s (login (:ip olt))]
    (let [s-new (agent/fh-onu s)]
      (try
        (doall (map #(merge {:in_bps 0 :out_bps 0 :in_bw 0 :out_bw 0 :rx_power -200}
                            %
                            (if (= "working" (:state %))
                              (optical-cmd-out s-new %)
                              {:rx_power -100}))
                    states))
        (catch Exception ex
          (println (str "caught exception: " (.getMessage ex)))
          (log/error (format "caught exception in olt-onu-rx for olt [%s][%s]: %s"
                             (:name olt) (:ip olt) (.getMessage ex)))
          nil)
        (finally (logout s))))))

(defn olt-onu-all
  [olt cards]
  (let [states (olt-onu-info olt cards false)]
    (olt-onu-rx-merge olt states)))

(defn states-for-onus
  "Get latest states for given onus"
  [client onus]
  (let [pons (distinct (map :pon onus))
        states (flatten
                (doall (for [pon pons]
                         (let [[islot iport]
                               (map read-string (str/split pon #"/"))]
                           (state-cmd-out client islot iport)))))]
    (map #(merge {:onu_id (:id %)} (get-state states (:pon %) (:oid %))) onus)))

(defn latest-states
  "Get latest state maps for given olt and onus"
  [olt onus]
  (log/info (format "Processing latest-states on [%s][%s], onu count: [%d]"
                    (:name olt) (:ip olt) (count onus)))
  (if-let [s (login (:ip olt))]
    (let [s-new (agent/fh-onu s)]
      (try
        (let [states (states-for-onus s-new onus)]
          (olt-onu-rx-merge olt states))
        (catch Exception ex
          (println (format "in latest-states caught exception: %s" (.getMessage ex)))
          (log/error (format "caught exception in olt-sn for olt [%s][%s]: %s"
                             (:name olt) (:ip olt)  (.getMessage ex))))
        (finally (logout s))))))

