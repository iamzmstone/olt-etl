(ns etl.olt-c300
  (:require [etl.telnet-agent :as agent]
            [cprop.core :refer [load-config]]
            [etl.parse :as parser]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

(def conf (load-config))
(def olt-login (:olt-login conf))
(def olt-pass (:olt-pass conf))

(defn no-paging
  "Send 'terminal length 0' command to make output no-paging"
  [session]
  (agent/cmd session "terminal length 0"))

(defn login
  "Telnet login olt and return session"
  ([ip port user pwd]
   (let [s (agent/login ip port user pwd)]
     (if s
       (do
         (no-paging s)
         s)
       (if (nil? s)
         (log/error (format "IP addess: [%s] is not reachable" ip))
         (log/error (format "Login failed at: [%s][%s][%s]" ip user pwd))))))
  ([ip user pwd]
   (login ip 23 user pwd)))

(defn logout
  "Disconnect telnet sesion"
  [session]
  (agent/quit session))

(defn cmd
  "Telnet to an OLT and get output of command, retry if it is not success"
  ([ip user pwd command retry]
   ;;;(println "Retry: " retry)
   (log/info (format "cmd: [%s] retry: [%d]" command retry))
   (if (> retry 0)
     (when-let [s (login ip user pwd)]
       (let [rst (agent/cmd-status s command)]
         (agent/quit s)
         (if (:status rst)
           (:result rst)
           (recur ip user pwd command (dec retry)))))))
  ([ip user pwd command]
   (cmd ip user pwd command 3)))

(defn card-info
  "Get output of command 'show card' for a given olt, and parse the output
  to form a vector of card map which is ready to insert to database"
  [olt]
  (log/info (format "card-info for olt [%s:%s]" (:name olt) (:ip olt)))
  (if-let [card-out (cmd (:ip olt) olt-login olt-pass "show card")]
    (map #(merge {:olt_id (:id olt)} %)
                 (parser/card-list card-out))))

(defn running-config
  "Get output of command 'show run' for a given olt"
  [olt]
  (log/info (format "running-config for olt [%s:%s]" (:name olt) (:ip olt)))
  (cmd (:ip olt) olt-login olt-pass "show run"))

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
  (if-let [s (login (:ip olt) olt-login olt-pass)]
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
  (if-let [s (login (:ip olt) olt-login olt-pass)]
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
  (if-let [s (login (:ip olt) olt-login olt-pass)]
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
    {:in_Bps 0 :out_Bps 0 :in_bw 0 :out_bw 0}))

(defn olt-onu-traffic
  "Call onu-traffic for each onu of onu list, and combine the outputs"
  [olt onu-list]
  (if-let [s (login (:ip olt) olt-login olt-pass)]
    (try
      (doall
       (map #(merge (select-keys % [:pon :oid]) (onu-traffic s %)) onu-list))
      (catch Exception ex
        (println (str "in olt-onu-traffic caught exception: " (.getMessage ex)))
        (log/error (format "caught exception in olt-onu-traffic for olt [%s][%s]: %s"
                          (:name olt) (:ip olt) (.getMessage ex))))
      (finally (logout s)))))

;;; code for get onu configuration
(defn onu-cmds [onu]
  "Get a list of olt commands for a given onu"
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

(defn onu-config [olt onu]
  "Get all related olt config for a given onu, and update its state in db"
  (let [s (login (:ip olt) olt-login olt-pass)
        cmds (onu-cmds onu)]
    (try
      (doall
       (zipmap (keys cmds)
               (map #(hash-map :cmd % :out (agent/cmd s %)) (vals cmds))))
      (catch Exception ex
        (println (format "caught exception in onu-config [%s][%s:%d] : %s"
                         (:name olt) (:pon onu) (:oid onu) (.getMessage ex))))
      (finally (logout s)))))

(defn state-new [onu onu-config]
  "Get a new onu map according to onu-config fetched"
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
(defn get-state
  "Get the state map from states via pon and oid given"
  [states pon oid]
  (first (filter #(and (= pon (:pon %)) (= oid (:oid %))) states)))

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
  (if-let [s (login (:ip olt) olt-login olt-pass)]
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
