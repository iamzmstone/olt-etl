(ns etl.olt
  (:require
   [etl.telnet-agent :as agent]
   [clojure.tools.logging :as log]))

(defn get-state
  "Get the state map from states via pon and oid given"
  [states pon oid]
  (first (filter #(and (= pon (:pon %)) (= oid (:oid %))) states)))

(defn no-paging
  "Send 'terminal length 0' command to make output no-paging"
  [session]
  (agent/cmd session "terminal length 0"))

(defn login
  "Telnet login olt and return session"
  ([ip port user pwd]
   (let [s (agent/login ip port user pwd)]
     (if s
       (do
         (no-paging s)
         s)
       (if (nil? s)
         (log/error (format "IP addess: [%s] is not reachable" ip))
         (log/error (format "Login failed at: [%s][%s][%s]" ip user pwd))))))
  ([ip user pwd]
   (login ip 23 user pwd)))

(defn login-en
  "Telnet login olt and enter enable mode, then return session"
  ([ip port user pwd]
   (let [s (agent/login-enable ip port user pwd)]
     (when-not s
       (if (nil? s)
         (log/error (format "IP addess: [%s] is not reachable" ip))
         (log/error (format "Login failed at: [%s][%s][%s]" ip user pwd))))
     s))
  ([ip user pwd]
   (login-en ip 23 user pwd)))

(defn logout
  "Disconnect telnet sesion"
  [session]
  (agent/quit session))

(defn cmd
  "Telnet to an OLT and get output of command, retry if it is not success"
  ([ip user pwd command retry]
   ;;;(println "Retry: " retry)
   (log/info (format "cmd: [%s] retry: [%d]" command retry))
   (when (> retry 0)
     (when-let [s (login ip user pwd)]
       (let [rst (agent/cmd-status s command)]
         (agent/quit s)
         (if (:status rst)
           (:result rst)
           (recur ip user pwd command (dec retry)))))))
  ([ip user pwd command]
   (cmd ip user pwd command 3)))
