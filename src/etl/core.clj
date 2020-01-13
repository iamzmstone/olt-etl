(ns etl.core
  (:require
   [etl.helper :refer [conf hw-olt? fh-olt?]]
   [etl.olt :as olt]
   [etl.olt-c300 :as c300]
   [etl.olt-huawei :as huawei]
   [etl.olt-fh :as fh]
   [etl.db :as db]
   [etl.telnet-agent :as telnet]
   [mount.core :as mount]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clj-time.core :as t]
   [clojure.tools.logging :as log]
   [etl.parse :as parser])
  (:gen-class))

(def cores (.. Runtime getRuntime availableProcessors))
(def conf-path (:conf-path conf))


(defn log-test []
  (log/info "log test ..."))

(defn olts-ok? []
  (let [olts (db/all-olts)]
    (doseq [olt olts]
      (when (not (telnet/reachable? (:ip olt)))
        (println (format "OLT is not reachable: [%s][%s]" (:name olt) (:ip olt)))))))

(defn zte-olts-login-ok? []
  (let [olts (db/zte-olts)]
    (doseq [olt olts]
      (println (:name olt) ":" (:ip olt))
      (let [s (olt/login (:ip olt) (:olt-login conf) (:olt-pass conf))]
        (if s
          (olt/logout s)
          (println (format "OLT login failed: [%s][%s]" (:name olt) (:ip olt))))))))

(defn other-olts-login-ok? []
  (let [olts (db/not-zte-olts)]
    (doseq [olt olts]
      (println (:name olt) ":" (:ip olt))
      (let [user (if (hw-olt? olt) (:hw-login conf) (:fh-login conf))
            pass (if (hw-olt? olt) (:hw-pass conf) (:fh-pass conf))
            s (olt/login-en (:ip olt) user pass)]
        (if s
          (olt/logout s)
          (println (format "OLT login failed: [%s][%s]" (:name olt) (:ip olt))))))))
    
(defn load-olts []
  (with-open [f (io/reader (conf :olt-txt-file))]
    (doseq [line (line-seq f)]
      (when (> (count line) 5)
        (if-let [[name ip] (str/split line #"\s+")]
          (db/save-olt {:name name :ip ip :brand "ZTE"}))))))

(defn load-tele-olts []
  (with-open [f (io/reader "/Users/zengm/test/telecom-olt.txt")]
    (doseq [line (line-seq f)]
      (when (> (count line) 5)
        (if-let [[name code room category brand ip] (str/split line #"\t")]
          (db/save-olt {:name name :ip ip :brand brand
                        :dev_code code :machine_room room :category category}))))))

(defn save-olt-conf
  "Save olt running config to file"
  [olt]
  (let [content (c300/running-config olt)]
    (with-open [f (io/writer (str conf-path (:ip olt) ".cfg"))]
      (.write f content))))

(defn dump-confs
  "Dump all olt's running config to local files"
  []
  (log/info "dump-confs start...")
  (doseq [olt (db/all-olts)]
    (future
      (save-olt-conf olt)
      (log/info (format "dump-confs finished...[%s][%s]" (:name olt) (:ip olt))))))

(defn olt-card-info
  "call different card-info func according to brand of olt"
  [olt]
  (cond
    (hw-olt? olt) (huawei/card-info olt)
    (fh-olt? olt) (fh/card-info olt)
    :else (c300/card-info olt)))

(defn etl-card-info []
  (let [olts (db/all-olts)]
    (log/info "etl-card-info start...")
    (doseq [card (remove nil? (flatten (doall (pmap olt-card-info olts))))]
      (db/save-card card))
    (log/info "etl-card-info finished...")))

(defn sn-info
  "for zte olt"
  ([olt retry]
   (log/info (format "processing sn-info [%s][%s][%d]" (:name olt) (:ip olt) retry))
   (if (> retry 0)
     (let [cards (db/olt-cards {:olt_id (:id olt)})
           sn-out (c300/olt-sn olt cards)]
       (if sn-out
         {:sns (map #(merge {:olt_id (:id olt)} %) (parser/sn-list sn-out))
          :pon-descs (map #(merge {:olt_id (:id olt)} %) (parser/pon-desc-list sn-out))}
         (recur olt (dec retry))))
     (log/error (format "Failed after retry in sn-info [%s][%s]"
                        (:name olt) (:ip olt)))))
  ([olt]
   (sn-info olt 3)))

(defn- card-sn-func
  [olt cards]
  (cond
    (hw-olt? olt) (huawei/olt-onu-info olt cards true)
    (fh-olt? olt) (fh/olt-onu-info olt cards true)
    :else nil))

(defn olt-sn-info
  "for huawei and fh olt"
  ([olt retry]
   (log/info (format "processing sn-info [%s][%s][%d]" (:name olt) (:ip olt) retry))
   (if (> retry 0)
     (let [cards (db/olt-cards {:olt_id (:id olt)})
           onus (card-sn-func olt (map :slot cards))]
       (if onus
         onus
         (recur olt (dec retry))))
     (log/error (format "Failed after retry in sn-info [%s][%s]"
                        (:name olt) (:ip olt)))))
  ([olt]
   (olt-sn-info olt 3)))

(defn- etl-onus-for-olts
  "Run etl onus for given olt list: for ZTE"
  [olts]
  (let [part-num (+ cores 2)]
    (log/info "zte-etl-onus start...")
    (doseq [[i olt-parts] (map-indexed vector (partition-all part-num olts))]
      (future
        (let [sn-maps (pmap sn-info olt-parts)]
          (doseq [onu (remove nil? (flatten (map :sns sn-maps)))]
            (db/save-onu onu))
          (doseq [pon-desc (remove nil? (flatten (map :pon-descs sn-maps)))]
            (db/upd-the-pon-desc pon-desc)))
        (log/info (format "zte-etl-onus finished at [%d]..." i))))))

(defn etl-zte-onus
  "Run etl onus for all zte olts"
  []
  (etl-onus-for-olts (db/zte-olts)))

(defn etl-olt-onus
  "Run etl onus for all hw&fh olts"
  []
  (let [part-num (+ cores 2)
        olts (db/not-zte-olts)]
    (log/info "etl-olt-onus start...")
    (doseq [[i olt-parts] (map-indexed vector (partition-all part-num olts))]
      (future
        (let [sns (pmap olt-sn-info olt-parts)]
          (doseq [onu (flatten sns)]
            (db/save-onu onu)))
        (log/info (format "etl-olt-onus finished at [%d]..." i))))))

(defn etl-onus-left
  "Run etl for olts without any onus"
  []
  (etl-onus-for-olts (db/olt-without-onus)))


(defn merge-onu-id [olt onus state]
  (let [onu (first (filter #(and
                             (= (:id olt) (:olt_id %))
                             (= (:pon state) (:pon %))
                             (= (:oid state) (:oid %)))
                           onus))]
    (merge {:olt (:name olt) :onu_id (:id onu)} state)))

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

(defn- olt-states-all
  ([olt retry]
   (log/info (format "processing olt-states-all [%s][%s][%d]"
                     (:name olt) (:ip olt) retry))
   (if (> retry 0)
     (let [cards (map :slot (db/olt-cards {:olt_id (:id olt)}))
           onus (db/olt-onus {:olt_id (:id olt)})
           states (if (hw-olt? olt)
                    (huawei/olt-onu-all olt cards)
                    (fh/olt-onu-all olt cards))]
       (if states
         (map #(merge-onu-id olt onus %) states)
         (recur olt (dec retry))))
     (log/error (format "Failed after retry in olt-states-all [%s][%s]"
                        (:name olt) (:ip olt)))))
  ([olt]
   (olt-states-all olt 3)))

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

(defn- etl-states-for-zte-olts 
  "Run etl states for a given ZTE olts list and a batch name"
  [olts batch-name]
  (let [batch_id (db/first-or-new-batch batch-name)
        part-num (+ cores 2)]
    (log/info (format "etl-zte-states for batch [%s] start..." batch-name))
    (doseq [[i olt-parts] (map-indexed vector (partition-all part-num olts))]
      (future
        (doseq [s (map #(merge {:batch_id batch_id} %)
                       (flatten (pmap all-state olt-parts)))]
          (if (:onu_id s)
            (db/save-state s)
            (log/warn "state without onu_id" s)))
        (log/info (format "etl-zte-states for batch [%s] finished at [%d]..."
                          batch-name i))
        (db/upd-batch {:batch_id batch_id :end_time (t/now) :finished true})))))

(defn- etl-states-for-non-zte-olts
  "Run etl states for a given non-ZTE olt list and a batch name"
  [olts batch-name]
  (let [batch_id (db/first-or-new-batch batch-name)
        part-num (+ cores 2)]
    (log/info (format "olt-states-all for batch [%s] start..." batch-name))
    (doseq [[i olt-parts] (map-indexed vector (partition-all part-num olts))]
      (future
        (doseq [s (map #(merge {:batch_id batch_id} %)
                       (flatten (pmap olt-states-all olt-parts)))]
          (if (:onu_id s)
            (db/save-state s)
            (log/warn "state without onu_id" s)))
        (log/info (format "olt-states-all for batch [%s] finished at [%d]..."
                          batch-name i))
        (db/upd-batch {:batch_id batch_id :end_time (t/now) :finished true})))))

(defn etl-zte-states
  "Run etl states for all ZTE olts"
  [batch-name]
  (etl-states-for-zte-olts (db/zte-olts) batch-name))

(defn etl-olt-states
  "Run etl states for all non-ZTE olts"
  [batch-name]
  (etl-states-for-non-zte-olts (db/not-zte-olts) batch-name))

(defn etl-states-left
  "Run etl states for olts without states for given batch-name"
  [batch-name]
  (let [olts (db/olt-without-states
              {:batch_id (db/first-or-new-batch batch-name)})]
    (etl-states-for-zte-olts olts batch-name)))

(defn- ls-func
  [olt onus]
  (cond
    (hw-olt? olt) (huawei/latest-states olt onus)
    (fh-olt? olt) (fh/latest-states olt onus)
    :else (c300/latest-states olt onus)))

(defn latest-states
  "Get the latest states of onus given"
  [onus]
  (log/info (format "latest-states for onus[%d] start..." (count onus)))
  (let [olt-onus (group-by :olt_id onus)
        olts (map #(db/get-olt-by-id {:id %}) (keys olt-onus))]
    (flatten (doall (pmap ls-func olts (vals olt-onus))))))

(defn etl-onu-names
  "Update onus' name for onus given"
  [onus]
  (let [olt-onus (group-by :olt_id onus)]
    (doseq [[olt_id onus] olt-onus]
      (let [olt (db/get-olt-by-id {:id olt_id})]
        (log/info (format "etl-onu-names for olt: [%s][%s][%d] start..."
                          (:name olt) (:ip olt) (count onus)))
        (future
          (db/batch-upd-onu-name (c300/olt-onu-name olt onus))
          (log/info (format "etl-onu-names for olt: [%s][%s] done..."
                            (:name olt) (:ip olt))))))))

(defn ls-test []
  (let [onus (random-sample 0.01 (etl.db/all-onus))]
    (latest-states onus)))

(defn etl-onu-without-name []
  (let [onus (db/zte-onus-without-name)]
    (etl-onu-names onus)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (mount/start)
  (case (str/lower-case (first args))
    "card" (etl-card-info)
    "pon-desc" (db/batch-init-pon-desc)
    "onu" (do
            (etl-zte-onus)
            (etl-olt-onus))
    "onu-left" (etl-onus-left)
    "onu-name" (etl-onu-without-name)
    "state" (let [bat-name (second args)]
              (etl-zte-states bat-name)
              (etl-olt-states bat-name))
    "state-left" (etl-states-left (second args))
    "reachable" (olts-ok?)
    "loginable" (do
                  (zte-olts-login-ok?)
                  (other-olts-login-ok?))
    (println "Wrong argument...")))
