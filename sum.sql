DROP PROCEDURE IF EXISTS `procedure_netflowip`;
DELIMITER $
CREATE PROCEDURE procedure_netflowip(IN cycle INT, IN dst_table VARCHAR(15), IN new_id INT)
BEGIN
    DECLARE insert_time, insert_flow INT;
    DECLARE insert_assetIp VARCHAR(16);

    DECLARE sum_time, old_flow INT;

    SET @dst_tbl = dst_table;

    SELECT time, flow, assetIp INTO insert_time, insert_flow, insert_assetIp FROM `netflowip` WHERE id = new_id;
    SET sum_time = ceil(insert_time / cycle) * cycle;

    PREPARE get_flow FROM "SELECT flow into old_flow FROM ? WHERE time = sum_time AND assetIp = insert_assetIp";
    EXECUTE  get_flow USING @dst_tbl;
    DEALLOCATE PREPARE get_flow;

    IF old_flow IS NOT NULL THEN
        PREPARE update_flow FROM "UPDATE ? SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIp = insert_assetIp";
        EXECUTE update_flow USING @dst_tbl;
        DEALLOCATE PREPARE update_flow;
    ELSE
        PREPARE insert_flow FROM "INSERT INTO ? (time, flow, assetIp) VALUES (sum_time, insert_flow, insert_assetIp)";
        EXECUTE insert_flow USING @dst_tbl;
        DEALLOCATE PREPARE insert_flow;
    END IF;
END
$
DELIMITER ;


DROP TRIGGER IF EXISTS `trigger_netflowip`;
DELIMITER $
CREATE TRIGGER `trigger_netflowip` AFTER INSERT ON `netflowip` FOR EACH ROW
BEGIN
    CALL procedure_netflowip(900, "netflowip_q", NEW.id);
END
$
DELIMITER ;
