-- :name add-olt :i!
-- :doc add a new olt record
INSERT INTO olts
(name, ip, brand, dev_code, machine_room, category)
VALUES (:name, :ip, :brand, :dev_code, :machine_room, :category)

-- :name upd-olt :! :n
-- :doc update olt by id
UPDATE olts
   SET name = :name,
       ip = :ip,
       brand = :brand
 WHERE id = :id

-- :name all-olts :? :*
-- :doc retrieve all olt records
SELECT * FROM olts
ORDER BY name

-- :name get-olt :? :1
-- :doc retrieve the otl by ip
SELECT * FROM olts
 WHERE ip = :ip

-- :name get-olt-by-id :? :1
-- :doc get the olt by id
SELECT * FROM olts
 WHERE id = :id

-- :name add-card :i!
-- :doc add a new card record
INSERT INTO cards
(olt_id, slot, card_type, model, port_cnt, hard_ver, soft_ver, status)
VALUES (:olt_id, :slot, :card_type, :model, :port_cnt, :hard_ver, :soft_ver, :status)

-- :name get-card :? :1
-- :doc retrieve the card by olt_id and slotno
SELECT * from cards
 WHERE olt_id = :olt_id
   AND slot = :slot

-- :name olt-cards :? :*
-- :doc retrieve all cards of a given olt
SELECT * FROM cards
 WHERE olt_id = :olt_id

-- :name upd-card :! :n
-- :doc update card information by id
UPDATE cards
   SET card_type = :card_type,
       model = :model,
       port_cnt = :port_cnt,
       hard_ver = :hard_ver,
       soft_ver = :soft_ver,
       status = :status
 WHERE id = :id

-- :name add-batch :i!
-- :doc add a new batch record
INSERT INTO batches
(name, start_time)
VALUES (:name, :start_time)

-- :name get-batch-by-name :? :1
-- :doc retrieve a batch record by name provided
SELECT * FROM batches
WHERE name = :name

-- :name latest-batch :? :1
-- :doc retrieve latest batch done
SELECT * FROM batches
 WHERE finished = true
 ORDER BY id DESC
 LIMIT 1
 
-- :name all-batches :? :*
-- :doc retrieve all batch records
SELECT * FROM batches

-- :name upd-batch :! :n
-- :doc update batch according to batch_id
UPDATE batches
   SET end_time = :end_time,
       finished = :finished
 WHERE id = :batch_id

-- :name all-onus :? :*
-- :doc retrieve all onus
SELECT * FROM onus

-- :name add-onu :i!
-- :doc add a new onu record
INSERT INTO onus
(olt_id, pon, oid, sn, type, auth, model)
VALUES (:olt_id, :pon, :oid, :sn, :type, :auth, :model)

-- :name get-onu-by-id :? :1
-- :doc retrieve onu record by id
SELECT * FROM onus
 WHERE id = :id

-- :name get-onu :? :1
-- :doc retrieve onu record by pon and oid
SELECT * FROM onus
 WHERE olt_id = :olt_id
   AND pon = :pon
   AND oid = :oid

-- :name upd-onu :! :n
-- :doc update onu according to onu id
UPDATE onus
   SET sn = :sn,
       type = :type,
       auth = :auth,
       model = :model
 WHERE id = :id

-- :name olt-onus :? :*
-- :doc retrieve all onus for a given olt
SELECT * from onus
 WHERE olt_id = :olt_id

-- :name add-state :i!
-- :doc add a new onu_state record
INSERT INTO onu_states
(onu_id, batch_id, state, rx_power, in_bps, out_bps, in_bw, out_bw)
VALUES (:onu_id, :batch_id, :state, :rx_power, :in_bps, :out_bps, :in_bw, :out_bw)

-- :name get-state :? :1
-- :doc retrieve state according to batch_id and onu_id
SELECT * from onu_states
 WHERE onu_id = :onu_id
   AND batch_id = :batch_id

-- :name upd-state :! :n
-- :doc update state according to id
UPDATE onu_states
   SET in_bw = :in_bw,
       out_bw = :out_bw,
       in_bps = :in_bps,
       out_bps = :out_bps,
       rx_power = :rx_power,
       state = :state
 WHERE id = :id

-- :name olt-without-onus :? :*
-- :doc retrieve olts has no onus
SELECT a.*
  FROM olts a
 WHERE NOT EXISTS (SELECT 1 FROM onus b
                    WHERE a.id = b.olt_id)

-- :name olt-without-states :? :*
-- :doc retrieve olts has no states for a given batch
SELECT a.*
  FROM olts a
 WHERE NOT EXISTS (SELECT 1 FROM onus b, onu_states c
                   WHERE a.id = b.olt_id
		   AND b.id = c.onu_id
		   AND c.batch_id = :batch_id)
		   

