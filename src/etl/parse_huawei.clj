(ns etl.parse-huawei
  (:require
   [clojure.string :as str]))

(defn parse-card
  "parse one line of card info"
  [line]
  (let [cols (str/split (str/trim line) #"\s+")
        t (get cols 1)
        m (if (= \E (get t 4)) "EPON" "GPON")
        cnt (if (#{"H802EPBD" "H808EPSD" "H802GPBD"} t) 8 16)]
    {:slot (read-string (get cols 0))
     :card_type t
     :port_cnt cnt
     :hard_ver nil
     :soft_ver nil
     :status (get cols 2)
     :model m}))

(defn card-list
  "Get a list of card map from output of show card"
  [card-section]
  (let [card-lines (filter #(re-find #"H\d\d\d(EP|GP|CG)" %)
                           (str/split-lines card-section))]
    (reduce conj [] (map parse-card card-lines))))

