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
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 * @version   SVN: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */

require_once 'PDB.php';
require_once 'Muuttaa/Exception.php';

/**
 * Common Muuttaa class
 *
 * Contains a number of helper methods, functions, etc. that are used by both
 * {@link Muuttaa} and {@link Muuttaa_Process}.
 *
 * @package   Muuttaa
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2008 Digg.com, Inc. 
 * @license   http://tinyurl.com/42zef New BSD License
 * @version   Release: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */
abstract class Muuttaa_Common
{
    /**
     * Name of queue
     *
     * @access protected
     * @var string $name Name of queue we're manipulating
     */
    protected $name = '';

    /**
     * The DB connection chosen
     *
     * @access protected
     * @var object $db Instance of {@link PDB}
     * @static
     */
    protected $db = null;

    /**
     * The host chosen to submit the job to
     *
     * @access protected
     * @var object $queue The host configuration object for the DB
     */
    protected $queue = null;

    /**
     * Constructor
     * 
     * @param string $name   Name of statement queue
     * @param mixed  $queues A single DB queue host or an array of hosts
     *
     * @throws InvalidArgumentException on invalid arguments
     * @return void
     * @see Muuttaa_Common::$name, Muuttaa_Common::$queue
     */
    public function __construct($name, $queues)
    {
        if (!is_string($name) || !strlen($name)) {
            throw new InvalidArgumentException('Invalid queue name');
        }

        $this->name = $name;

        if (is_array($queues)) {
            // Randomly select a host to insert a job into.
            shuffle($queues);
            $this->queue = array_shift($queues);
        } else {
            $this->queue = $queues;
        }

        if (!self::isValidHost($this->queue)) {
            throw new InvalidArgumentException(
                'Host is not properly formatted'
            );
        }
    }

    /**
     * Get a DB connection
     *
     * @return object Instance of {@link PDB}
     * @see PDB::connect(), PDB::setFetchMode(), Muuttaa_Common::$db
     * @see Muuttaa_Common::dsn()
     */
    protected function db()
    {
        if (isset($this->db)) {
            return $this->db;
        }

        $this->db = PDB::connect(self::dsn($this->queue), 
                                 $this->queue->user, 
                                 $this->queue->pass);

        $this->db->setFetchMode(PDO::FETCH_OBJ);
        return $this->db;
    }

    /**
     * Build a {@link PDB} compatible DSN
     *
     * @param object $host An object that conforms to the host interface
     * 
     * @return string
     * @see Muuttaa_Common::isValidHost()
     */
    static protected function dsn($host)
    {
        // Build the DSN for PDB
        return $host->type . ':' .
               'host=' . $host->host . ';' . 
               'port=' . $host->port . ';' . 
               'dbname=' . $host->name;
    }

    /**
     * Execute SQL against a host
     *
     * Execute a query inside of a transaction against the queue that we're
     * currently working against.
     *
     * @param string $sql  The query to run
     * @param array  $args The arguments for the query
     *
     * @throws Various exceptions when an error occurs
     * @return void
     */
    protected function execute($sql, array $args = array()) 
    {
        $this->db()->beginTransaction();
        try {
            $this->db()->query($sql, $args);
            $this->db()->commit();
        } catch (Exception $e) {
            $this->db()->rollBack();
            throw $e;
        }
    }

    /**
     * Does $host conform to the exptected interface?
     * 
     * @param object $host The host object to validate 
     * 
     * @return boolean True if it conforms, false if it does not
     */
    static public function isValidHost($host)
    {
        return (is_object($host) && 
                isset($host->type) && 
                isset($host->host) &&
                isset($host->port) && 
                isset($host->name) &&
                isset($host->user) &&
                isset($host->pass));
    }

    /**
     * Get the {@link Muuttaa} table name
     *
     * @param string $table Which Muuttaa related table to return
     * 
     * @return string The table name properly formatted
     * @see Muuttaa_Common::$name
     */
    protected function getTable($table = 'statements')
    {
        return sprintf('muuttaa_%s_%s', $this->name, $table);
    }

    /**
     * Get the contents of the queue
     *
     * @param int $offset Offset to start fetching statements from
     * @param int $limit  Max. number of statements to fetch
     * @param int $status If not null, return statements in this
     *                    status. If null, return statements regardless
     *                    of status.
     *
     * @return array Queue contents
     *
     * @see {@link Muuttaa_Statement::STATUS_PENDING}
     * @see {@link Muuttaa_Statement::STATUS_COMPLETE}
     * @see {@link Muuttaa_Statement::STATUS_FAILED}
     * @see {@link Muuttaa_Statement::STATUS_EXCEPTION}
     */
    public function getQueue($offset = 0, $limit = 50, $status = null)
    {
        $sql  = sprintf("SELECT * FROM `%s` ", $this->getTable());
        $args = array();
        if ($status !== null) {
            $sql   .= "WHERE `status` = ? ";
            $args[] = $status;
        }

        $sql .= sprintf("ORDER BY `date_created` DESC LIMIT %d, %d;",
                        $offset, $limit);
        $res = $this->db()->query($sql, $args);
        $out = array();
        while ($row = $res->fetchObject()) {
            $out[$row->id] = $row;
        }
        return $out;
    }

    /**
     * Get queue size
     *
     * @return int Queue size
     */
    public function getQueueSize()
    {
        static $stmt;
        if (!isset($stmt)) {
            $sql  = "SELECT COUNT(*) FROM `%s`;";
            $stmt = $this->db()->prepare(sprintf($sql, $this->getTable()));
        }
        $stmt->execute();
        $col = $stmt->fetchColumn(0);
        return $col[0];
    }

    /**
     * Get queue status
     *
     * @return array Array of PDO status => number of statements
     */
    public function getQueueStats()
    {
        static $stmt;
        if (!isset($stmt)) {
            $sql = "SELECT `status`, COUNT(*) as `num` FROM `%s` " .
                "GROUP BY `status`;";
            $stmt = $this->db()->prepare(sprintf($sql, $this->getTable()));
        }
        $stmt->execute();
        $out = array(Muuttaa_Statement::STATUS_PENDING   => 0,
                     Muuttaa_Statement::STATUS_COMPLETE  => 0,
                     Muuttaa_Statement::STATUS_FAILED    => 0,
                     Muuttaa_Statement::STATUS_EXCEPTION => 0);
        while ($row = $stmt->fetch(PDO::FETCH_OBJ)) {
            $out[$row->status] = $row->num;
        }
        return $out;
    }

    /**
     * Destroy a queue
     *
     * @return void
     */
    public function destroy()
    {
        $tables = array($this->getTable(), $this->getTable('errors'));
        foreach ($tables as $table) {
            $this->db()->query(sprintf("DROP TABLE `%s`;", $table));
        }
    }

    /**
     * Close all of the DB connections
     *
     * @return void
     * @see Muuttaa_Common::$db
     */
    public function __destruct()
    {
        $this->db = null;
    }
}

?>
