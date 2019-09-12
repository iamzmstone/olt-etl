(ns etl.telnet-agent
  (:require [clj-telnet.core :as telnet]
            [clojure.string :as cs]))

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

(defn login
  "Telnet to ip:port and login with user:pwd, return client
  object with session and prompt"
  ([ip port user pwd]
   (let [s (telnet/get-telnet ip port)]
     (do
       (telnet/read-all s)
       (telnet/write s (str user "\n"))
       (telnet/read-all s)
       (telnet/write s (str pwd "\n"))
       {:session s :prompt (last (cs/split-lines (telnet/read-all s)))})))
  ([ip user pwd]
   (login ip 23 user pwd)))

(defn cmd
  "Send command to session and return output"
  ([client command prompt]
   (let [prompts (prompts prompt)]
     (do
       (telnet/write (:session client) command)
       (telnet/read-until-or
        (:session client) prompts 60000))))
  ([client command]
   (let [prompt (:prompt client)]
     (cmd client command prompt))))

(defn cmd-status
  "Send command to session and return status and output"
  ([client command prompt]
   (let [prompts (prompts prompt)]
     (do
       (telnet/write (:session client) command)
       (let [rst (telnet/read-until-or
                  (:session client) prompts 180000)]
         (if (some (partial cs/ends-with? rst) prompts)
           {:status true :result rst}
           {:status false :result rst})))))
  ([client command]
   (let [prompt (:prompt client)]
     (cmd-status client command prompt))))

(defn quit
  "Quit/Disconnect from telnet session"
  [client]
  (telnet/kill-telnet (:session client)))
