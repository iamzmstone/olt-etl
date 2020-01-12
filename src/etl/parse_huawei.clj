(ns etl.parse-huawei
  (:require
   [clojure.string :as str]))

(defn parse-card
  "parse one line of card info"
  [line]
  (let [cols (str/split (str/trim line) #"\s+")]
    {:slot (read-string (get cols 0))
     :card_type (get cols 1)
     :port_cnt 16
     :hard_ver nil
     :soft_ver nil
     :status (get cols 2)
     :model "GPON"}))

(defn card-list
  "Get a list of card map from output of show card"
  [card-section]
  (let [card-lines (filter #(re-find #"H\d\d\dGPFD" %)
                           (str/split-lines card-section))]
    (reduce conj [] (map parse-card card-lines))))

