(ns etl.helper
  (:require
   [cprop.core :refer [load-config]]))

(def conf (load-config))

(defn stateof
  "Get the state of given state according to state mapping rules"
  [s]
  (let [m (:state-mapping conf)]
    (or (m s) s)))

(defn hw-olt?
  [olt]
  (= "华为技术有限公司" (:brand olt)))

(defn fh-olt?
  [olt]
  (= "烽火通信科技股份有限公司" (:brand olt)))
