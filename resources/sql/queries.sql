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

-- :name zte-olts :? :*
-- :doc retrieve all ZTE olts
SELECT * FROM olts
 WHERE brand = "中兴通讯股份有限公司"

-- :name not-zte-olts :? :*
-- :doc retrieve all other brand olts
SELECT * FROM olts
 WHERE brand != "中兴通讯股份有限公司"

-- :name get-olt :? :1
-- :doc retrieve the olt by ip
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
   AND model in ('EPON', 'GPON')

-- :name olt-uplink-card :? :*
-- :doc retrieve all uplink cards of a given olts
SELECT * FROM cards
 WHERE olt_id = :olt_id
   AND model = 'UPLINK'

-- :name all-cards :? :*
-- :doc retrieve all cards
SELECT * FROM cards

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

-- :name add-pon-desc :i!
-- :doc add a new pon desc record
INSERT INTO pon_desc
(olt_id, pon)
VALUES (:olt_id, :pon)

-- :name get-pon-desc :? :1
-- :doc retrieve the pon_desc record by olt_id and pon
SELECT * from pon_desc
 WHERE olt_id = :olt_id
   AND pon = :pon

-- :name upd-pon-desc :! :n
-- :doc update pon desc record by id
UPDATE pon_desc
   SET name = :name
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

-- :name zte-onus-without-name :? :*
-- :doc retrieve onus without name
SELECT a.*
  FROM onus a, olts b
 WHERE a.name = ''
   AND a.olt_id = b.id
   AND b.brand = '中兴通讯股份有限公司'

-- :name olt-onus :? :*
-- :doc retrieve onus by olt_id
SELECT * FROM onus
 WHERE olt_id = :olt_id

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
       model = :model,
       upd_time = now()
 WHERE id = :id

-- :name upd-onu-name :! :n
-- :doc update onu name according to onu id
UPDATE onus
   SET name = :name
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
   AND a.brand = "中兴通讯股份有限公司"

-- :name olt-without-states :? :*
-- :doc retrieve olts has no states for a given batch
SELECT a.*
  FROM olts a
 WHERE NOT EXISTS (SELECT 1 FROM onus b, onu_states c
                   WHERE a.id = b.olt_id
		   AND b.id = c.onu_id
		   AND c.batch_id = :batch_id)
   AND a.brand = "中兴通讯股份有限公司"		   

-- :name get-uplink-state :? :1
-- :doc retrieve the uplink-state by card_id
SELECT * from uplink_states
 WHERE card_id = :card_id

-- :name add-uplink-state :i!
-- :doc create a new uplink-state record
INSERT INTO uplink_states(card_id, port_state, port_rx)
VALUES (:card_id, :port_state, :port_rx)

-- :name upd-uplink-state :! :n
-- :doc update uplink-state according to id
UPDATE uplink_states
   SET card_id = :card_id,
       port_state = :port_state,
       port_rx = :port_rx,
       upd_time = now()
 WHERE id = :id
