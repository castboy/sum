DROP PROCEDURE IF EXISTS `procedure_netflowip`;
DELIMITER $
CREATE PROCEDURE procedure_netflowip(IN cycle INT, IN dst_table VARCHAR(15), IN new_id INT)
BEGIN
    DECLARE insert_time, insert_flow INT;
    DECLARE insert_assetIp VARCHAR(16);

    DECLARE sum_time, old_flow INT;

    SELECT time, flow, assetIp INTO insert_time, insert_flow, insert_assetIp FROM `netflowip` WHERE id = new_id;
    SET sum_time = ceil(insert_time / cycle) * cycle;


    CASE dst_table
	WHEN "netflowip_q" THEN
	    SELECT flow into old_flow FROM netflowip_q WHERE time = sum_time AND assetIp = insert_assetIp;
	    IF old_flow IS NOT NULL THEN
		UPDATE netflowip_q SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIp = insert_assetIp;
	    ELSE
		insert into netflowip_q(time, flow, assetip) values (sum_time, insert_flow, insert_assetip);
	    end if;
        WHEN "netflowip_h" THEN
	    SELECT flow into old_flow FROM netflowip_h WHERE time = sum_time AND assetIp = insert_assetIp;
	    IF old_flow IS NOT NULL THEN
		UPDATE netflowip_h SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIp = insert_assetIp;
	    ELSE
		insert into netflowip_h(time, flow, assetip) values (sum_time, insert_flow, insert_assetip);
	    end if;
        ELSE
	    SELECT flow into old_flow FROM netflowip_d WHERE time = sum_time AND assetIp = insert_assetIp;
	    IF old_flow IS NOT NULL THEN
		UPDATE netflowip_d SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIp = insert_assetIp;
	    ELSE
		insert into netflowip_d(time, flow, assetip) values (sum_time, insert_flow, insert_assetip);
	    end if;
        END CASE;
END
$
DELIMITER ;


DROP TRIGGER IF EXISTS `trigger_netflowip`;
DELIMITER $
CREATE TRIGGER `trigger_netflowip` AFTER INSERT ON `netflowip` FOR EACH ROW
BEGIN
    CALL procedure_netflowip(60 * 15, "netflowip_q", NEW.id);
    CALL procedure_netflowip(60 * 60, "netflowip_h", NEW.id);
    CALL procedure_netflowip(60 * 60 * 24, "netflowip_d", NEW.id);
END
$
DELIMITER ;
