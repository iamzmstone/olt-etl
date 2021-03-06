(ns etl.olt-c300
  (:require
   [etl.helper :refer [conf]]
   [etl.telnet-agent :as agent]
   [etl.olt :refer [login logout cmd get-state]]
   [etl.parse :as parser]
   [clojure.tools.logging :as log]
   [clojure.string :as str]))

(defn command
  [ip cmd-str]
  (cmd ip (:olt-login conf) (:olt-pass conf) cmd-str))

(defn logon
  [ip]
  (login ip (:olt-login conf) (:olt-pass conf)))

(defn card-info
  "Get output of command 'show card' for a given olt, and parse the output
  to form a vector of card map which is ready to insert to database"
  [olt]
  (log/info (format "card-info for olt [%s:%s]" (:name olt) (:ip olt)))
  (if-let [card-out (command (:ip olt) "show card")]
    (map #(merge {:olt_id (:id olt)} %)
                 (parser/card-list card-out))))

(defn running-config
  "Get output of command 'show run' for a given olt"
  [olt]
  (log/info (format "running-config for olt [%s:%s]" (:name olt) (:ip olt)))
  (command (:ip olt) "show run"))

(defn pon-onu-sn
  "Send 'show run int [g|e]pon-olt_1/$pon' command and get onu sn on a given pon port"
  [session pon model]
  (let [m (str/lower-case model)]
    (agent/cmd session (format "show run int %s-olt_1/%s" m pon))))

(defn card-sn
  "call pon-onu-sn for each pon port on a given card, and combine the output to
  get sn output for a given card"
  [session card]
  (str/join "\n"
            (map #(pon-onu-sn session % (:model card))
                 (map #(format "%d/%d" (:slot card) %)
                      (range 1 (inc (:port_cnt card)))))))

(defn olt-sn
  "call card-sn for each card on a given olt, and combine the output to get
  all sn output of the olt"
  [olt cards]
  (if-let [s (logon (:ip olt))]
    (try
      (str/join "=====\n"
                (map #(card-sn s %)
                     (remove #(nil? (:model %)) cards)))
      (catch Exception ex
        (println (str "caught exception: " (.getMessage ex)))
        (log/error (format "caught exception in olt-sn for olt [%s][%s]: %s"
                           (:name olt) (:ip olt) (.getMessage ex)))
        nil)
      (finally (logout s)))))

(defn pon-state
  "Get output of command 'show gpon onu state g|epon-olt_1/' for a given pon port"
  [session pon model]
  (let [state-cmd
        (cond
          (= (str/lower-case model) "epon") (format "show onu all-status epon-olt_1/%s"
                                                    pon)
          :else (format "show gpon onu state gpon-olt_1/%s" pon))]
    (agent/cmd session state-cmd)))

(defn olt-state
  "call pon-state for each pon port for a given olt, and combine all outputs"
  [olt pon-ports]
  (if-let [s (logon (:ip olt))]
    (try
      (str/join "=====\n"
                (map #(pon-state s (:pon %) (:model %)) pon-ports))
      (catch Exception ex
        (println (str "in olt-state caught exception: " (.getMessage ex)))
        (log/error (format "caught exception in olt-state for olt [%s][%s]: %s"
                           (:name olt) (:ip olt) (.getMessage ex))))
      (finally (logout s)))))

(defn onu-rx-power
  "Send 'show pon power onu-rx' command and get optical rx power info"
  [session onu]
  (if (= "working" (:state onu))
    (let [out (agent/cmd session (format "show pon power onu-rx %s-onu_1/%s:%d"
                                         (:model onu) (:pon onu) (:oid onu)))]
      (parser/rx-map (str/split-lines out)))
    {:pon (:pon onu) :oid (:oid onu) :rx_power -100}))

(defn olt-rx-power
  "Call onu-rx-power for each onu of onu list, and combine the outputs"
  [olt onu-list]
  (if-let [s (logon (:ip olt))]
    (try
      (doall (map #(onu-rx-power s %) onu-list))
      (catch Exception ex
        (println (str "in olt-rx-power caught exception: " (.getMessage ex)))
        (log/error (format "caught exception in olt-rx-power for olt [%s][%s]: %s"
                          (:name olt) (:ip olt) (.getMessage ex))))
      (finally (logout s)))))

(defn onu-traffic
  "Send 'show int gpon-onu_1/$pon:$oid' command and get traffc information"
  [session onu]
  (if (= "working" (:state onu))
    (parser/traffic-map
     (str/split-lines
      (agent/cmd session (format "show int %s-onu_1/%s:%s"
                                 (:model onu) (:pon onu) (:oid onu)))))
    {:in_bps 0 :out_bps 0 :in_bw 0 :out_bw 0}))

(defn olt-onu-traffic
  "Call onu-traffic for each onu of onu list, and combine the outputs"
  [olt onu-list]
  (if-let [s (logon (:ip olt))]
    (try
      (doall
       (map #(merge (select-keys % [:pon :oid]) (onu-traffic s %)) onu-list))
      (catch Exception ex
        (println (str "in olt-onu-traffic caught exception: " (.getMessage ex)))
        (log/error (format "caught exception in olt-onu-traffic for olt [%s][%s]: %s"
                          (:name olt) (:ip olt) (.getMessage ex))))
      (finally (logout s)))))

;;; code for getting description/name of onus
(defn onu-name
  "Get the description/name from output of onu config for a given onu"
  [session onu]
  (let [m (:model onu)
        pon (:pon onu)
        oid (:oid onu)
        cmd (format "show run int %s-onu_1/%s:%d" m pon oid)]
    {:id (:id onu)
     :name (or (parser/gbk-str (parser/onu-name (agent/cmd session cmd) m))
               "NULL")}))

(defn olt-onu-name
  "Call onu-name for each onu of onu list and output onu-name list"
  [olt onus]
  (if-let [s (logon (:ip olt))]
    (try
      (doall
       (map #(onu-name s %) onus))
      (catch Exception ex
        (println (str "in olt-onu-name caught exception: " (.printStackTrace ex)))
        (log/error (format "caught exception in olt-onu-name for olt [%s][%s]: %s"
                           (:name olt) (:ip olt) (.getMessage ex))))
      (finally (logout s)))))

;;; code for onu configuration
(defn onu-cmds
  "Get a list of olt commands for a given onu"
  [onu]
  (let [m (:model onu)
        pon (:pon onu)
        oid (:oid onu)]
   {:sn (format "show run int %s-olt_1/%s" m pon)
    :state (case m
             "epon" (format "show onu all-status epon-olt_1/%s" pon)
             (format "show gpon onu state gpon-olt_1/%s" pon))
    :onu-cfg (format "show run int %s-onu_1/%s:%d" m pon oid)
    :running-cfg (format "show onu running config %s-onu_1/%s:%d" m pon oid)
    :onu-rx (format "show pon power onu-rx %s-onu_1/%s:%d" m pon oid)
    :traffic (format "show int %s-onu_1/%s:%s" m pon oid)
    :mac (format "show mac %s onu %s-onu_1/%s:%d" m m pon oid)
    :uncfg "show pon onu uncfg sn loid"}))

(defn onu-config
  "Get all related olt config for a given onu, and update its state in db"
  [olt onu]
  (let [s (logon (:ip olt))
        cmds (onu-cmds onu)]
    (try
      (doall
       (zipmap (keys cmds)
               (map #(hash-map :cmd %
                               :out (parser/gbk-str (agent/cmd s %))) (vals cmds))))
      (catch Exception ex
        (println (format "caught exception in onu-config [%s][%s:%d] : %s"
                         (:name olt) (:pon onu) (:oid onu) (.getMessage ex))))
      (finally (logout s)))))

(defn state-new
  "Get a new onu map according to onu-config fetched"
  [onu onu-config]
  (let [pon (:pon onu) oid (:oid onu)
        pon-str (format "/%s:%d" pon oid)
        state-out (get-in onu-config [:state :out])
        sn-out (get-in onu-config [:sn :out])
        rx-out (get-in onu-config [:onu-rx :out])
        tfc-out (get-in onu-config [:traffic :out])
        ms (parser/parse-status
            (first (filter #(re-find (re-pattern pon-str) %)
                           (str/split-lines state-out))))
        msn (parser/onu-sn
             (first (filter #(re-find (re-pattern (format "onu %d type" oid)) %)
                            (str/split-lines sn-out))))
        mrx (parser/rx-map (str/split-lines rx-out))
        mtfc (parser/traffic-map (str/split-lines tfc-out))]
    (merge ms msn mrx mtfc onu)))

(defn onu-data [olt onu]
  (let [conf (onu-config olt onu)
        new (state-new onu conf)]
    {:state (dissoc new :upd_time) :conf conf}))


;;; code for get latest state information for given onus list
(defn states-for-onus
  "Get latest states for given onus"
  [session onus]
  (let [pon-ports (distinct (map #(select-keys % [:pon :model]) onus))
        state-out (str/join "=====\n"
                            (map #(pon-state session (:pon %) (:model %)) pon-ports))
        states (parser/onu-state-list state-out)]
    (map #(merge {:onu_id (:id %)} (get-state states (:pon %) (:oid %))) onus)))

(defn rx-for-onus
  "Get latest rx power list for given onus"
  [session onus]
  (doall (map #(onu-rx-power session %)
              onus)))

(defn trfc-for-onus
  "Get latest traffic list for given onus"
  [session onus]
  (doall (map #(onu-traffic session %)
              onus)))

(defn latest-states
  "Get latest state maps for given olt and onus"
  [olt onus]
  (log/info (format "Processing latest-states on [%s][%s], onu count: [%d]"
                    (:name olt) (:ip olt) (count onus)))
  (if-let [s (logon (:ip olt))]
    (try
      (let [states (states-for-onus s onus)
            rxs (rx-for-onus s states)
            trfcs (trfc-for-onus s states)]
        (map merge states rxs trfcs))
      (catch Exception ex
        (println (format "in latest-states caught exception: %s" (.getMessage ex)))
        (log/error (format "caught exception in olt-sn for olt [%s][%s]: %s"
                           (:name olt) (:ip olt)  (.getMessage ex))))
      (finally (logout s)))))

(defn cutover-states
  "Get cutover state(only state and rx) maps for given olt and onus"
  [olt onus]
  (log/info (format "Processing cutover-states on [%s][%s], onu count: [%d]"
                    (:name olt) (:ip olt) (count onus)))
  (if-let [s (logon (:ip olt))]
    (try
      (let [states (states-for-onus s onus)
            rxs (rx-for-onus s states)]
        (map merge states rxs))
      (catch Exception ex
        (println (format "in latest-states caught exception: %s" (.getMessage ex)))
        (log/error (format "caught exception in cutover-states for olt [%s][%s]: %s"
                           (:name olt) (:ip olt)  (.getMessage ex))))
      (finally (logout s)))))

;;; code for uplink fibers
(defn uplink-name
  [type i]
  (cond
    (and (not= type "GUFQ") (< i 2)) "xgei"
    :else "gei"))

(defn uplink-state-cmd
  [type slot i]
  (let [name (uplink-name type i)]
    (format "show int %s_1/%d/%d" name slot (inc i))))

(defn uplink-rx-power-cmd
  [type slot i]
  (let [name (uplink-name type i)]
    (format "show int optical-module-info %s_1/%d/%d" name slot (inc i))))

(defn uplink-state-cmd-out
  [session uplink-card]
  (doall
   (for [i (range (:port_cnt uplink-card))]
     (parser/uplink-state
      (agent/cmd session
                 (uplink-state-cmd
                  (:card_type uplink-card)
                  (:slot uplink-card)
                  i))))))

(defn uplink-rx-power-cmd-out
  [session uplink-card]
  (doall
   (for [i (range (:port_cnt uplink-card))]
     (parser/uplink-rx-power
      (agent/cmd session
                 (uplink-rx-power-cmd
                  (:card_type uplink-card)
                  (:slot uplink-card)
                  i))))))

(defn olt-uplink-states
  [olt uplinks]
  (log/info (format "Processing olt-uplink-states on [%s][%s]"
                    (:name olt) (:ip olt)))
  (if-let [s (logon (:ip olt))]
    (try
      (doall (for [uplink uplinks]
               {:olt_id (:id olt)
                :card_id (:id uplink)
                :uplink (format "%s:%d" "UPLINK" (:slot uplink))
                :state (uplink-state-cmd-out s uplink)
                :rx_power (uplink-rx-power-cmd-out s uplink)}))
      (catch Exception ex
        (println (format "in olt-uplink-states caught exception: %s" (.getMessage ex)))
        (log/error (format "caught exception in cutover-states for olt [%s][%s]: %s"
                           (:name olt) (:ip olt)  (.getMessage ex))))
      (finally (logout s)))))
