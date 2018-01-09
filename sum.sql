#netflowip
DROP PROCEDURE IF EXISTS `procedure_netflowip`;
DELIMITER $
CREATE PROCEDURE procedure_netflowip(IN cycle INT, IN dst_table VARCHAR(15), IN new_id INT)
BEGIN
    DECLARE insert_time, insert_flow INT;
    DECLARE insert_assetIP VARCHAR(16);

    DECLARE sum_time, old_flow INT;

    SELECT time, flow, assetIP INTO insert_time, insert_flow, insert_assetIp FROM `netflowip` WHERE id = new_id;
    SET sum_time = ceil(insert_time / cycle) * cycle;

    CASE dst_table
    	WHEN "netflowip_q" THEN
    	    SELECT flow INTO old_flow FROM netflowip_q WHERE time = sum_time AND assetIP = insert_assetIP;
    	    IF old_flow IS NOT NULL THEN
    		      UPDATE netflowip_q SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIP = insert_assetIP;
    	    ELSE
    		      INSERT INTO netflowip_q(time, flow, assetIP) VALUES (sum_time, insert_flow, insert_assetIP);
    	    END IF;
        WHEN "netflowip_h" THEN
    	    SELECT flow INTO old_flow FROM netflowip_h WHERE time = sum_time AND assetIP = insert_assetIP;
    	    IF old_flow IS NOT NULL THEN
    		      UPDATE netflowip_h SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIP = insert_assetIP;
    	    ELSE
    		      INSERT INTO netflowip_h(time, flow, assetIP) VALUES (sum_time, insert_flow, insert_assetIP);
    	    END IF;
        ELSE
    	    SELECT flow INTO old_flow FROM netflowip_d WHERE time = sum_time AND assetIP = insert_assetIP;
    	    IF old_flow IS NOT NULL THEN
    		      UPDATE netflowip_d SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIP = insert_assetIP;
    	    ELSE
    		      INSERT INTO netflowip_d(time, flow, assetIP) VALUES (sum_time, insert_flow, insert_assetIP);
    	    END IF;
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



#netflowd
DROP PROCEDURE IF EXISTS `procedure_netflowd`;
DELIMITER $
CREATE PROCEDURE procedure_netflowd(IN cycle INT, IN dst_table VARCHAR(15), IN new_id INT)
BEGIN
    DECLARE insert_time, insert_flow INT;
    DECLARE insert_direction VARCHAR(16);

    DECLARE sum_time, old_flow INT;

    SELECT time, flow, direction INTO insert_time, insert_flow, insert_direction FROM `netflowd` WHERE id = new_id;
    SET sum_time = ceil(insert_time / cycle) * cycle;

    CASE dst_table
    	WHEN "netflowd_q" THEN
    	    SELECT flow INTO old_flow FROM netflowd_q WHERE time = sum_time AND direction = insert_direction;
    	    IF old_flow IS NOT NULL THEN
    		      UPDATE netflowd_q SET flow = insert_flow + old_flow WHERE time = sum_time AND direction = insert_direction;
    	    ELSE
    		      INSERT INTO netflowd_q(time, flow, direction) VALUES (sum_time, insert_flow, insert_direction);
    	    END IF;
        WHEN "netflowd_h" THEN
            SELECT flow INTO old_flow FROM netflowd_h WHERE time = sum_time AND direction = insert_direction;
            IF old_flow IS NOT NULL THEN
                  UPDATE netflowd_h SET flow = insert_flow + old_flow WHERE time = sum_time AND direction = insert_direction;
            ELSE
                  INSERT INTO netflowd_h(time, flow, direction) VALUES (sum_time, insert_flow, insert_direction);
            END IF;
        ELSE
            SELECT flow INTO old_flow FROM netflowd_d WHERE time = sum_time AND direction = insert_direction;
            IF old_flow IS NOT NULL THEN
                  UPDATE netflowd_d SET flow = insert_flow + old_flow WHERE time = sum_time AND direction = insert_direction;
            ELSE
                  INSERT INTO netflowd_d(time, flow, direction) VALUES (sum_time, insert_flow, insert_direction);
            END IF;
    END CASE;
END
$
DELIMITER ;

DROP TRIGGER IF EXISTS `trigger_netflowd`;
DELIMITER $
CREATE TRIGGER `trigger_netflowd` AFTER INSERT ON `netflowd` FOR EACH ROW
BEGIN
    CALL procedure_netflowd(60 * 15, "netflowd_q", NEW.id);
    CALL procedure_netflowd(60 * 60, "netflowd_h", NEW.id);
    CALL procedure_netflowd(60 * 60 * 24, "netflowd_d", NEW.id);
END
$
DELIMITER ;





#netflowp
DROP PROCEDURE IF EXISTS `procedure_netflowp`;
DELIMITER $
CREATE PROCEDURE procedure_netflowp(IN cycle INT, IN dst_table VARCHAR(15), IN new_id INT)
BEGIN
    DECLARE insert_time, insert_flow INT;
    DECLARE insert_protocol VARCHAR(16);

    DECLARE sum_time, old_flow INT;

    SELECT time, flow, protocol INTO insert_time, insert_flow, insert_protocol FROM `netflowp` WHERE id = new_id;
    SET sum_time = ceil(insert_time / cycle) * cycle;

    CASE dst_table
    	WHEN "netflowp_q" THEN
    	    SELECT flow INTO old_flow FROM netflowp_q WHERE time = sum_time AND protocol = insert_protocol;
    	    IF old_flow IS NOT NULL THEN
    		      UPDATE netflowp_q SET flow = insert_flow + old_flow WHERE time = sum_time AND protocol = insert_protocol;
    	    ELSE
    		      INSERT INTO netflowp_q(time, flow, protocol) VALUES (sum_time, insert_flow, insert_protocol);
    	    END IF;
        WHEN "netflowp_h" THEN
            SELECT flow INTO old_flow FROM netflowp_h WHERE time = sum_time AND protocol = insert_protocol;
            IF old_flow IS NOT NULL THEN
                  UPDATE netflowp_h SET flow = insert_flow + old_flow WHERE time = sum_time AND protocol = insert_protocol;
            ELSE
                  INSERT INTO netflowp_h(time, flow, protocol) VALUES (sum_time, insert_flow, insert_protocol);
            END IF;
        ELSE
            SELECT flow INTO old_flow FROM netflowp_d WHERE time = sum_time AND protocol = insert_protocol;
            IF old_flow IS NOT NULL THEN
                  UPDATE netflowp_d SET flow = insert_flow + old_flow WHERE time = sum_time AND protocol = insert_protocol;
            ELSE
                  INSERT INTO netflowp_d(time, flow, protocol) VALUES (sum_time, insert_flow, insert_protocol);
            END IF;
    END CASE;
END
$
DELIMITER ;

DROP TRIGGER IF EXISTS `trigger_netflowp`;
DELIMITER $
CREATE TRIGGER `trigger_netflowp` AFTER INSERT ON `netflowp` FOR EACH ROW
BEGIN
    CALL procedure_netflowp(60 * 15, "netflowp_q", NEW.id);
    CALL procedure_netflowp(60 * 60, "netflowp_h", NEW.id);
    CALL procedure_netflowp(60 * 60 * 24, "netflowp_d", NEW.id);
END
$
DELIMITER ;




#netflowdp
DROP PROCEDURE IF EXISTS `procedure_netflowdp`;
DELIMITER $
CREATE PROCEDURE procedure_netflowdp(IN cycle INT, IN dst_table VARCHAR(15), IN new_id INT)
BEGIN
    DECLARE insert_time, insert_flow INT;
    DECLARE insert_protocol VARCHAR(16);
    DECLARE insert_direction VARCHAR(16);

    DECLARE sum_time, old_flow INT;

    SELECT time, flow, protocol, direction INTO insert_time, insert_flow, insert_protocol, insert_direction FROM `netflowdp` WHERE id = new_id;
    SET sum_time = ceil(insert_time / cycle) * cycle;

    CASE dst_table
    	WHEN "netflowdp_q" THEN
    	    SELECT flow INTO old_flow FROM netflowdp_q WHERE time = sum_time AND protocol = insert_protocol AND direction = insert_direction;
    	    IF old_flow IS NOT NULL THEN
    		      UPDATE netflowdp_q SET flow = insert_flow + old_flow WHERE time = sum_time AND protocol = insert_protocol AND direction = insert_direction;
    	    ELSE
    		      INSERT INTO netflowdp_q(time, flow, protocol, direction) VALUES (sum_time, insert_flow, insert_protocol, insert_direction);
    	    END IF;
        WHEN "netflowdp_h" THEN
            SELECT flow INTO old_flow FROM netflowdp_h WHERE time = sum_time AND protocol = insert_protocol AND direction = insert_direction;
            IF old_flow IS NOT NULL THEN
                  UPDATE netflowdp_h SET flow = insert_flow + old_flow WHERE time = sum_time AND protocol = insert_protocol AND direction = insert_direction;
            ELSE
                  INSERT INTO netflowdp_h(time, flow, protocol, direction) VALUES (sum_time, insert_flow, insert_protocol, insert_direction);
            END IF;
        ELSE
            SELECT flow INTO old_flow FROM netflowdp_d WHERE time = sum_time AND protocol = insert_protocol AND direction = insert_direction;
            IF old_flow IS NOT NULL THEN
                  UPDATE netflowdp_d SET flow = insert_flow + old_flow WHERE time = sum_time AND protocol = insert_protocol AND direction = insert_direction;
            ELSE
                  INSERT INTO netflowdp_d(time, flow, protocol, direction) VALUES (sum_time, insert_flow, insert_protocol, insert_direction);
            END IF;
    END CASE;
END
$
DELIMITER ;

DROP TRIGGER IF EXISTS `trigger_netflowdp`;
DELIMITER $
CREATE TRIGGER `trigger_netflowdp` AFTER INSERT ON `netflowdp` FOR EACH ROW
BEGIN
    CALL procedure_netflowdp(60 * 15, "netflowdp_q", NEW.id);
    CALL procedure_netflowdp(60 * 60, "netflowdp_h", NEW.id);
    CALL procedure_netflowdp(60 * 60 * 24, "netflowdp_d", NEW.id);
END
$
DELIMITER ;



#netflowipd
DROP PROCEDURE IF EXISTS `procedure_netflowipd`;
DELIMITER $
CREATE PROCEDURE procedure_netflowipd(IN cycle INT, IN dst_table VARCHAR(15), IN new_id INT)
BEGIN
    DECLARE insert_time, insert_flow INT;
    DECLARE insert_assetIP VARCHAR(16);
    DECLARE insert_direction VARCHAR(16);

    DECLARE sum_time, old_flow INT;

    SELECT time, flow, assetIP, direction INTO insert_time, insert_flow, insert_assetIP, insert_direction FROM `netflowipd` WHERE id = new_id;
    SET sum_time = ceil(insert_time / cycle) * cycle;

    CASE dst_table
    	WHEN "netflowipd_q" THEN
    	    SELECT flow INTO old_flow FROM netflowipd_q WHERE time = sum_time AND assetIP = insert_assetIP AND direction = insert_direction;
    	    IF old_flow IS NOT NULL THEN
    		      UPDATE netflowipd_q SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIP = insert_assetIP AND direction = insert_direction;
    	    ELSE
    		      INSERT INTO netflowipd_q(time, flow, assetIP, direction) VALUES (sum_time, insert_flow, insert_assetIP, insert_direction);
    	    END IF;
        WHEN "netflowipd_h" THEN
            SELECT flow INTO old_flow FROM netflowipd_h WHERE time = sum_time AND assetIP = insert_assetIP AND direction = insert_direction;
            IF old_flow IS NOT NULL THEN
                  UPDATE netflowipd_h SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIP = insert_assetIP AND direction = insert_direction;
            ELSE
                  INSERT INTO netflowipd_h(time, flow, assetIP, direction) VALUES (sum_time, insert_flow, insert_assetIP, insert_direction);
            END IF;
        ELSE
            SELECT flow INTO old_flow FROM netflowipd_d WHERE time = sum_time AND assetIP = insert_assetIP AND direction = insert_direction;
            IF old_flow IS NOT NULL THEN
                  UPDATE netflowipd_d SET flow = insert_flow + old_flow WHERE time = sum_time AND assetIP = insert_assetIP AND direction = insert_direction;
            ELSE
                  INSERT INTO netflowipd_d(time, flow, assetIP, direction) VALUES (sum_time, insert_flow, insert_assetIP, insert_direction);
            END IF;
    END CASE;
END
$
DELIMITER ;

DROP TRIGGER IF EXISTS `trigger_netflowipd`;
DELIMITER $
CREATE TRIGGER `trigger_netflowipd` AFTER INSERT ON `netflowipd` FOR EACH ROW
BEGIN
    CALL procedure_netflowipd(60 * 15, "netflowipd_q", NEW.id);
    CALL procedure_netflowipd(60 * 60, "netflowipd_h", NEW.id);
    CALL procedure_netflowipd(60 * 60 * 24, "netflowipd_d", NEW.id);
END
$
DELIMITER ;
