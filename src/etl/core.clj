(ns etl.core
  (:require [etl.olt-c300 :as c300]
            [etl.db :as db]
            [mount.core :as mount]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clj-time.core :as t]
            [cprop.core :refer [load-config]]
            [clojure.tools.logging :as log]
            [etl.parse :as parser])
  (:gen-class))

(def conf (load-config))

(defn log-test []
  (log/info "log test ..."))

(defn load-olts []
  (with-open [f (io/reader (conf :olt-txt-file))]
    (doseq [line (line-seq f)]
      (if (> (count line) 5)
        (if-let [[name ip] (str/split line #"\s+")]
          (db/save-olt {:name name :ip ip :brand "ZTE"}))))))

(defn etl-card-info []
  (let [olts (db/all-olts)]
    (log/info "etl-card-info start...")
    (doseq [card (flatten (pmap c300/card-info olts))]
      (db/save-card card))
    (log/info "etl-card-info finished...")))

(defn sn-info
  ([olt retry]
   (log/info (format "processing sn-info [%s][%s][%d]" (:name olt) (:ip olt) retry))
   (if (> retry 0)
     (let [cards (db/olt-cards {:olt_id (:id olt)})
           sn-out (c300/olt-sn olt cards)]
       (if sn-out
         (map #(merge {:olt_id (:id olt)} %) (parser/sn-list sn-out))
         (recur olt (dec retry))))
     (log/error (format "Failed after retry in sn-info [%s][%s]"
                        (:name olt) (:ip olt)))))
  ([olt]
   (sn-info olt 3)))

(defn etl-onus []
  (let [olts (db/all-olts)]
    (log/info "etl-onus start...")
    (doseq [onu (flatten (pmap sn-info olts))]
      (db/save-onu onu))
    (log/info "etl-onus finished...")))

(defn merge-onu-id [olt onus state]
  (let [onu (first (filter #(and
                             (= (:id olt) (:olt_id %))
                             (= (:pon state) (:pon %))
                             (= (:oid state) (:oid %)))
                           onus))]
    (merge {:onu_id (:id onu)} state)))

(defn state-info
  ([olt retry]
   (log/info (format "processing state-info [%s][%s][%d]" (:name olt) (:ip olt) retry))
   (if (> retry 0)
     (let [onus (db/olt-onus {:olt_id (:id olt)})
           pon-ports (distinct (map #(select-keys % [:pon :model]) onus))
           state-out (c300/olt-state olt pon-ports)]
       (if state-out
         (map #(merge-onu-id olt onus %)
              (parser/onu-state-list state-out))
         (recur olt (dec retry))))
     (log/error (format "Failed after retry in state-info [%s][%s]"
                        (:name olt) (:ip olt)))))
  ([olt]
   (state-info olt 3)))

(defn rx-power-info
  ([olt onus retry]
   (log/info (format "processing rx-power-info [%s][%s][%d]" (:name olt) (:ip olt) retry))
   (if (> retry 0)
     (if-let [rx-list (c300/olt-rx-power olt onus)]
       rx-list
       (recur olt onus (dec retry)))
     (log/error (format "Failed after retry in rx-power-info [%s][%s]"
                        (:name olt) (:ip olt)))))
  ([olt onus]
   (rx-power-info olt onus 3)))

(defn traffic-info
  ([olt onus retry]
   (log/info (format "processing traffic-info [%s][%s][%d]" (:name olt) (:ip olt) retry))
   (if (> retry 0)
     (if-let [traffics (c300/olt-onu-traffic olt onus)]
       traffics
       (recur olt onus (dec retry)))
     (log/error (format "Failed after retry in traffic-info [%s][%s]"
                        (:name olt) (:ip olt)))))
  ([olt onus]
   (traffic-info olt onus 3)))

(defn all-state [olt]
  (let [onus (state-info olt)]
    (map merge onus (rx-power-info olt onus) (traffic-info olt onus))))

(defn etl-states [batch-name]
  (let [olts (db/all-olts)
        batch_id (db/first-or-new-batch batch-name)]
    (log/info (format "etl-states for batch [%s] start..." batch-name))
    (doseq [s (map #(merge {:batch_id batch_id} %)
                   (flatten (pmap all-state olts)))]
      (if (:onu_id s)
        (db/save-state s)
        (log/warn "state without onu_id" s)))
    (db/upd-batch {:batch_id batch_id :end_time (t/now) :finished true})
    (log/info (format "etl-states for batch [%s] finished..." batch-name))))

(defn latest-states
  "Get the latest states of onus given"
  [onus]
  (log/info (format "latest-states for onus[%d] start..." (count onus)))
  (let [olt-onus (partition-by :olt_id onus)
        olts (map #(db/get-olt-by-id {:id (:olt_id (first %))}) olt-onus)]
    (flatten (pmap c300/latest-states olts olt-onus))))

(defn mytest []
  (let [onus (random-sample 0.01 (etl.db/all-onus))]
    (latest-states onus)))
  
(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (mount/start)
  (case (str/lower-case (first args))
    "card" (etl-card-info)
    "onu" (etl-onus)
    "state" (etl-states (second args))
    (println "Wrong argument..."))
  (System/exit 0))
