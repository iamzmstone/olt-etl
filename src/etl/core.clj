(ns etl.core
  (:require [etl.olt-c300 :as c300]
            [etl.db :as db]
            [mount.core :as mount]
            [clojure.string :as str]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [etl.parse :as parser])
  (:gen-class))


(defn log-test []
  (log/info "log test ..."))

(defn etl-card-info []
  (let [olts (db/all-olts)]
    (log/info "etl-card-info start...")
    (doseq [card (flatten (pmap c300/card-info olts))]
      (db/save-card card))
    (log/info "etl-card-info finished...")))

(defn sn-info [olt]
  (let [cards (c300/card-info olt)]
    (map #(merge {:olt_id (:id olt)} %) (parser/sn-list (c300/olt-sn olt cards)))))

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

(defn state-info [olt]
  (let [onus (db/olt-onus {:olt_id (:id olt)})
        pon-ports (distinct (map #(select-keys % [:pon :model]) onus))]
    (map #(merge-onu-id olt onus %)
         (parser/onu-state-list (c300/olt-state olt pon-ports)))))

(defn rx-power-info [olt onus]
  (c300/olt-rx-power olt onus))

(defn traffic-info [olt onus]
  (c300/olt-onu-traffic olt onus))

(defn all-state [olt]
  (let [onus (state-info olt)]
    (map merge onus (rx-power-info olt onus) (traffic-info olt onus))))

(defn etl-states [batch-name]
  (let [olts (db/all-olts)
        batch_id (db/first-or-new-batch batch-name)]
    (log/info (format "etl-states for batch [%s] start..." batch-name))
    (doseq [s (map #(merge {:batch_id batch_id} %)
                   (flatten (pmap all-state olts)))]
      (db/save-state s))
    (db/upd-batch {:batch_id batch_id :end_time (t/now) :finished true})
    (log/info (format "etl-states for batch [%s] finished..." batch-name))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (mount/start)
  (case (str/lower-case (first args))
    "card" (etl-card-info)
    "onu" (etl-onus)
    "state" (etl-states (second args))))
