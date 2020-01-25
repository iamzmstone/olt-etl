(ns etl.parse-fh
  (:require [clojure.string :as str]))

(defn parse-card
  "parse one line of card info"
  [line]
  (let [cols (str/split (str/trim line) #"\s+")
        t (get cols 1)
        c (if (= t "HU2A") 4 16)
        m (if (= t "HU2A") "UPLINK" "GPON")]
    {:slot (read-string (get cols 0))
     :card_type t
     :port_cnt c
     :hard_ver (get cols 2)
     :soft_ver (get cols 3)
     :status nil
     :model m}))

(defn card-list
  "Get a list of card map from output of show card"
  [card-section]
  (let [card-lines (filter #(re-find #"(GCOB|HU2A)" %)
                           (str/split-lines card-section))]
    (reduce conj [] (map parse-card card-lines))))

(defn parse-rx-power
  "parse rx-power section to map"
  [section]
  (if-let [rx-line (first (filter #(re-find #"^RECV POWER" %)
                                  (str/split-lines section)))]
    (let [items (str/split rx-line #"\s+")]
      {:rx_power (read-string (get items 3))})))
