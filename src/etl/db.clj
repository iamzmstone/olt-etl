(ns etl.db
  (:require [cprop.core :refer [load-config]]
            [clj-time.jdbc]
            [clojure.java.jdbc :as jdbc]
            [java-time.pre-java8 :as jt]
            [conman.core :as conman]
            [mount.core :refer [defstate]]))

(def conf (load-config))
(def pool-spec
  {:jdbc-url (:database-url conf)})

(defstate ^:dynamic *db*
  :start (conman/connect! pool-spec)
  :stop (conman/disconnect! *db*))

(conman/bind-connection *db* "sql/queries.sql")

(defn first-or-new-batch [name]
  (if-let [bat (get-batch-by-name {:name name})]
    (:id bat)
    (:generated_key (add-batch {:name name :start_time (java.util.Date.)}))))

(defn- keys-card [card]
  (select-keys card [:port_cnt :card_type :port_cnt :status]))

(defn save-card [card]
  "Add a new card if it doesn't exist in db, otherwise if it is changed, update
it in db"
  (if-let [card-in-db (get-card card)]
    (if (not (= (keys-card card) (keys-card card-in-db)))
      (upd-card (merge {:id (:id card-in-db)} card)))
    (add-card card)))

(defn- keys-onu [onu]
  (select-keys onu [:sn :type :auth]))

(defn save-onu [onu]
  "Add a new onu if it doesn't exist in db, otherwise update it in db if it diff"
  (if-let [onu-in-db (get-onu onu)]
    (if (not (= (keys-onu onu) (keys-onu onu-in-db)))
      (upd-onu (merge {:id (:id onu-in-db)} onu)))
    (add-onu onu)))

(defn save-state [state]
  "Add a new state if it doesn't exist in db, otherwise update it"
  (if-let [state-in-db (get-state state)]
    (upd-state (merge {:id (:id state-in-db)} state))
    (add-state state)))

(extend-protocol jdbc/IResultSetReadColumn
  java.sql.Timestamp
  (result-set-read-column [v _2 _3]
    (.toLocalDateTime v))
  java.sql.Date
  (result-set-read-column [v _2 _3]
    (.toLocalDate v))
  java.sql.Time
  (result-set-read-column [v _2 _3]
    (.toLocalTime v)))

(extend-protocol jdbc/ISQLValue
  java.util.Date
  (sql-value [v]
    (java.sql.Timestamp. (.getTime v)))
  java.time.LocalTime
  (sql-value [v]
    (jt/sql-time v))
  java.time.LocalDate
  (sql-value [v]
    (jt/sql-date v))
  java.time.LocalDateTime
  (sql-value [v]
    (jt/sql-timestamp v))
  java.time.ZonedDateTime
  (sql-value [v]
    (jt/sql-timestamp v)))
