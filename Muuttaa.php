<?php

/**
 * Muuttaa is a MySQL manipulation queue 
 *
 * PHP version 5.2.0+
 *
 * LICENSE: This source file is subject to the New BSD license that is 
 * available through the world-wide-web at the following URI:
 * http://www.opensource.org/licenses/bsd-license.php. If you did not receive  
 * a copy of the New BSD License and are unable to obtain it through the web, 
 * please send a note to license@php.net so we can mail you a copy immediately.
 *
 * @package   Muuttaa
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2008, 2009 Digg.com, Inc. 
 * @license   http://tinyurl.com/42zef New BSD License
 * @version   SVN: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */

require_once 'Muuttaa/Common.php';
require_once 'Muuttaa/Statement.php';

/**
 * The main Muuttaa class for queueing up MySQL queries
 *
 * @package   Muuttaa
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2008 Digg.com, Inc. 
 * @license   http://tinyurl.com/42zef New BSD License
 * @version   Release: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */
class Muuttaa extends Muuttaa_Common
{
    /**
     * Statments to queue up
     *
     * @access private
     * @var array $statements Statements to queue up for processing
     * @see Muuttaa_Statement
     */
    protected $statements = array();

    /**
     * Schemas for queues
     *
     * @access private
     * @var array $schema Table schemas for queues
     * @see Muuttaa::__construct()
     */
    protected $schema = array(
        "CREATE TABLE IF NOT EXISTS `muuttaa_%s_statements` (
             `id` bigint(20) unsigned not null auto_increment,
             `hosts` text not null,
             `statement` text not null,
             `tries` tinyint(2) unsigned not null default '0',
             `retries` tinyint(2) unsigned not null default '0',
             `status` tinyint(1) unsigned not null,
             `date_created` timestamp not null default CURRENT_TIMESTAMP,
             PRIMARY KEY (`id`),
             KEY (`status`)
         ) ENGINE=InnoDB CHARSET=utf8",

        "CREATE TABLE IF NOT EXISTS `muuttaa_%s_errors` (
             `id` bigint(20) unsigned not null auto_increment,
             `statementid` bigint(20) unsigned not null,
             `code` int(9) unsigned not null,
             `message` text not null,
             `query` text not null,
             `date_created` timestamp not null default CURRENT_TIMESTAMP,
             PRIMARY KEY (`id`),
             KEY (`statementid`),
             KEY (`date_created`)
         ) ENGINE=InnoDB CHARSET=utf8"
    );

    /**
     * Constructor
     * 
     * @param string $name   Name of statement queue
     * @param mixed  $queues A single DB queue host or an array of hosts
     *
     * @throws {@link Muuttaa_Exception} on bad name/host
     * @return void
     * @see Muuttaa_Common::execute()
     */
    public function __construct($name, $queues)
    {
        parent::__construct($name, $queues);

        // Initialize this queue table if we haven't already.
        foreach ($this->schema as $table) {
            $this->execute(sprintf($table, $this->name));
        }
    }

    /**
     * Add a statement to the queue
     *
     * @param object $stmt An instance of {@link Muuttaa_Statement}
     * 
     * @throws {@link Muuttaa_Exception} on error
     * @return boolean True if all went well
     */
    public function addStatement(Muuttaa_Statement $stmt) 
    {
        $cnt = count($this->statements);
        $res = array_push($this->statements, $stmt);

        if ($res != ($cnt + 1)) {
            throw new Muuttaa_Exception('Could not append statement');
        }

        return true;
    }

    /**
     * Commit statements to queue
     *
     * @access public
     * @throws {@link Muuttaa_Exception} on query error
     * @return array Array of (STATEMENT_ID => STATEMENT)
     */
    public function commit()
    {
        if (!count($this->statements)) {
            return true;
        }

        $sql = 'INSERT INTO muuttaa_%s_statements
                SET hosts = ?,
                    statement = ?,
                    retries = ?,
                    status = ?,
                    date_created = NOW()';

        $db = $this->db();
        $db->beginTransaction();
        $out = array();
        try {
            $sth = $db->prepare(sprintf($sql, $this->name));
            foreach ($this->statements as $stmt) {
                $res = $sth->execute(array(json_encode($stmt->getHosts()),
                                           json_encode($stmt->getQueries()),
                                           $stmt->getRetries(),
                                           Muuttaa_Statement::STATUS_PENDING));
                $out[$db->lastInsertId()] = $stmt;
            }
            $db->commit();
        } catch (Exception $e) {
            $db->rollBack();
            throw $e;
        }

        $this->statements = array();
        return $out;
    }
}

?>
