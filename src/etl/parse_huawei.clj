(ns etl.parse-huawei
  (:require
   [clojure.string :as str]))

(defn- is-uplink?
  [type]
  (#{"H801GICF" "H801X2CS"} type))

(defn parse-card
  "parse one line of card info"
  [line]
  (let [cols (str/split (str/trim line) #"\s+")
        t (get cols 1)
        m (cond
            (is-uplink? t) "UPLINK"
            (= \E (get t 4)) "EPON"
            :else "GPON")
        cnt (cond
              (= m "UPLINK") 2
              (#{"H802EPBD" "H808EPSD" "H802GPBD"} t) 8
              :else 16)]
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
  (let [card-lines (filter #(re-find #"H\d\d\d(EP|GP|CG|GICF|X2CS)" %)
                           (str/split-lines card-section))]
    (reduce conj [] (map parse-card card-lines))))

(defn uplink-rx-power
  [cmd-out]
  (if-let [rx-str (second (re-find #"(?m)RX power\(dBm\)\s+:\s+([\d-\.]+)" cmd-out))]
    (let [rx (read-string rx-str)]
      (if (number? rx)
        rx
        -100))
    -404))
