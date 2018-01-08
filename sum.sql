DROP PROCEDURE IF EXISTS `procedure_netflowip`;
DELIMITER $
CREATE PROCEDURE procedure_netflowip(IN interval INT, IN dst_talbe VARCHAR(15))
BEGIN
    DECLARE insert_time, insert_flow INT
    DECLARE insert_assetIp VARCHAR(16)

    DECLARE sum_time, old_flow INT

    SELECT time, flow, assetIp INTO insert_time, insert_flow, insert_assetIp FROM `netflowip` WHERE id = NEW.id;
    SET sum_time = ceil(insert_time / interval) * interval
    SELECT flow into old_flow FROM `dst_talbe` WHERE time = sum_time AND assetIp = insert_assetIp;
    IF old_flow IS NOT NULL THEN
        UPDATE `dst_talbe` SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIp = insert_assetIp;
    ELSE
        INSERT INTO `dst_talbe` (time, flow, assetIp) VALUES (sum_time, insert_flow, insert_assetIp)
    END IF
END
$
DELIMITER ;


DROP TRIGGER IF EXISTS `trigger_netflowip`;
DELIMITER $
CREATE TRIGGER `trigger_netflowip` AFTER INSERT ON `netflowip` FOR EACH ROW
BEGIN
    CALL procedure_netflow(900, "netflowip_q")
END
$
DELIMITER ;
