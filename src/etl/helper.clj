(ns etl.helper
  (:require
   [clojure.string :as str]
   [mount.core :refer [defstate]]
   [cprop.core :refer [load-config]]))

(defstate conf
  :start (load-config))

(defn stateof
  "Get the state of given state according to state mapping rules"
  [s]
  (let [m (:state-mapping (load-config))]
    (or (m s) s)))

(defn hw-olt?
  [olt]
  (= "华为技术有限公司" (:brand olt)))

(defn fh-olt?
  [olt]
  (= "烽火通信科技股份有限公司" (:brand olt)))

(defn uplink-states
  [rxs]
  (map #(if (> % -40) "up" "down") rxs))

(defn db-uplink-state
  [uplink-state]
  {:card_id (:card_id uplink-state)
   :port_state (str/join "," (:state uplink-state))
   :port_rx (str/join "," (:rx_power uplink-state))})
