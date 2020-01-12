(ns etl.telnet-agent
  (:require [clj-telnet.core :as telnet]
            [clojure.string :as cs])
  (:import [java.net InetAddress]))

(def ends [\# \> \] \)])

(defn repl-last-char
  "replace last character of string with char parameter"
  [s c]
  (cs/join "" (conj (into [] (butlast s)) c)))

(defn prompts
  "return vector of prompts with different ends"
  [prompt]
  (distinct
   (into [prompt]
         (map #(repl-last-char prompt %) ends))))

(defn reachable? [ip]
  (.isReachable (InetAddress/getByName ip) 5000))

(defn login
  "Telnet to ip:port and login with user:pwd, return client
  object with session and prompt"
  ([ip port user pwd]
   (when (reachable? ip)
     (let [s (telnet/get-telnet ip port)]
         (telnet/read-all s)
         (telnet/write s (str user))
         (telnet/read-all s)
         (telnet/write s (str pwd))
         (let [prompt (last (cs/split-lines (telnet/read-all s)))]
           (if (some #(= prompt %) ["Username:" "Password:"])
             false    ;;; login failed
             {:session s :prompt prompt})))))
  ([ip user pwd]
   (login ip 23 user pwd)))

(defn login-enable
  "Telnet to ip:port and login with user:pwd, return client
  object with session and prompt"
  ([ip port user pwd]
   (when (reachable? ip)
     (let [s (telnet/get-telnet ip port)]
         (telnet/read-all s)
         (telnet/write s (str user))
         (telnet/read-all s)
         (telnet/write s (str pwd))
         (telnet/read-all s)
         (telnet/write s "terminal length 0")  ; for fiberhome
         (telnet/read-all s)
         (telnet/write s "undo smart")   ; for huawei
         (telnet/read-all s)
         (telnet/write s "scroll")  ; for huawei
         (telnet/read-all s)
         ;;; enable mode
         (telnet/write s "enable")
         (telnet/read-all s)
         (telnet/write s (str pwd))
         (let [prompt (last (cs/split-lines (telnet/read-all s)))]
           (if (some #(= prompt %) ["Username:" "Password: " ""])
             false    ;;; login failed
             {:session s :prompt prompt})))))
  ([ip user pwd]
   (login-enable ip 23 user pwd)))

(defn fh-onu
  "Send 'cd onu' command and return a new client with new prompt"
  [client]
  (let [s (:session client)]
    (telnet/write s "cd onu")
    (let [prompt (last (cs/split-lines (telnet/read-all s)))]
      {:session s :prompt prompt})))

(defn hw-config
  "Send config and interface gpon 0/n command then return client with new prompt"
  [client slot]
  (let [s (:session client)]
    (telnet/write s "config")
    (telnet/read-all s)
    (telnet/write s (format "interface gpon 0/%d" slot))
    (let [prompt (last (cs/split-lines (telnet/read-all s)))]
      {:session s :prompt prompt})))

(defn cmd
  "Send command to session and return output"
  ([client command prompt]
   (let [prompts (prompts prompt)]
       (telnet/write (:session client) command)
       (telnet/read-until-or
        (:session client) prompts 60000)))
  ([client command]
   (let [prompt (:prompt client)]
     (cmd client command prompt))))

(defn cmd-status
  "Send command to session and return status and output"
  ([client command prompt]
   (let [prompts (prompts prompt)]
       (telnet/write (:session client) command)
       (let [rst (telnet/read-until-or
                  (:session client) prompts 300000)]
         (if (some (partial cs/ends-with? rst) prompts)
           {:status true :result rst}
           {:status false :result rst}))))
  ([client command]
   (let [prompt (:prompt client)]
     (cmd-status client command prompt))))

(defn quit
  "Quit/Disconnect from telnet session"
  [client]
  (telnet/kill-telnet (:session client)))
