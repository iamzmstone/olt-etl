(ns etl.helper
  (:require
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
