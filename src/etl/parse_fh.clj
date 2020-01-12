(ns etl.parse-fh
  (:require [clojure.string :as str]))

(defn parse-card
  "parse one line of card info"
  [line]
  (let [cols (str/split (str/trim line) #"\s+")]
    {:slot (read-string (get cols 0))
     :card_type (get cols 1)
     :port_cnt 16
     :hard_ver (get cols 2)
     :soft_ver (get cols 3)
     :status nil
     :model "GPON"}))

(defn card-list
  "Get a list of card map from output of show card"
  [card-section]
  (let [card-lines (filter #(re-find #"GCOB" %)
                           (str/split-lines card-section))]
    (reduce conj [] (map parse-card card-lines))))

(defn parse-rx-power
  "parse rx-power section to map"
  [section]
  (if-let [rx-line (first (filter #(re-find #"^RECV POWER" %)
                                  (str/split-lines section)))]
    (let [items (str/split rx-line #"\s+")]
      {:rx_power (read-string (get items 3))})))
