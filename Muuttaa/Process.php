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
require_once 'Muuttaa/Exception/FailedLock.php';
require_once 'Muuttaa/Exception/FailedOnRetry.php';
require_once 'Muuttaa/Exception/FailedQuery.php';

/**
 * Process Muuttaa statements from a random queue
 *
 * @package   Muuttaa
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2008 Digg.com, Inc. 
 * @license   http://tinyurl.com/42zef New BSD License
 * @version   Release: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */
class Muuttaa_Process extends Muuttaa_Common
{
    /**
     * Links to hosts
     *
     * A {@link Muuttaa_Statement} can be ran against any host. It can be
     * ran against many hosts as well. This array holds database connections
     * to the hosts which the statements are being ran on.
     *
     * NOTE: This should not be confused with the queues where statements are
     * put while they await processing. These are the hosts which the 
     * statements have specified they should be ran against.
     *
     * @access protected
     * @var array $links An array of DB connections to hosts
     * @static
     */
    protected $links = array();

    /**
     * Constructor
     *
     * Take a list of queue servers and a list of queue names and randomly
     * choose one to munch on.
     *
     * @param string $names  An array of queues to munch on
     * @param mixed  $queues A single DB queue host or an array of hosts
     *
     * @return void
     * @see Muuttaa_Common
     */
    public function __construct($names, $queues)
    {
        if (is_array($names)) {
            // Randomly select one of the queues to process through
            shuffle($names);
            $name = array_shift($names); 
        } else {
            $name = $names;
        }

        parent::__construct($name, $queues);
    }

    /**
     * Process a number of {@link Muuttaa_Statement}'s
     *
     * This will establish a user lock in the queue host and then start 
     * working on the transactions. Depending on the various exceptions thrown
     * it will either mark the transaction as failed, retry it or log an
     * error.
     *
     * While it's processing through the statements it keeps track of how
     * long those queries took and then sleeps for a little bit between
     * each statement. The multiplier is used to increase the amount of time
     * to sleep beyond a 1:1 ratio.
     *
     * @param integer $limit      The number of statements to process 
     * @param float   $multiplier Multiply the microseconds it took to run the
     *                            statement by this to come up with a sane
     *                            amount of breathing room
     *
     * @return void 
     * @see Muuttaa_Process::lock(), Muuttaa_Process::unlock()
     * @see Muuttaa_Statement
     * @throws OutOfRangeException on invalid limit
     */
    public function process($limit = 50, $multiplier = 1.5)
    {
        if ((int)$limit <= 0) {
            throw new OutOfRangeException('Limit value is incorrect');
        }

        $this->lock();

        $result = $this->getQueue(0, $limit, Muuttaa_Statement::STATUS_PENDING);
        foreach ($result as $stmt) {
            try {
                $a = microtime(true);
                if ((int)$stmt->tries > (int)$stmt->retries) {
                    throw new Muuttaa_Exception_FailedOnRetry();
                }

                $this->runStatement($stmt);
                $this->updateStatus($stmt->id, 
                                    Muuttaa_Statement::STATUS_COMPLETE);
                
                $b = microtime(true);

                // Take the seconds + miliseconds, convert to microseconds and
                // then multiply by our multiplier. 
                $c = (int)((($b - $a) * 1000000) * (float)$multiplier);

                // Sleep for a number of microseconds depending on our 
                // multiplier.
                usleep($c); 
            } catch (Muuttaa_Exception_FailedOnRetry $e) {
                // We've exhausted all efforts to get this query to properly
                // run so we're killing it and moving on.
                $this->updateStatus($stmt->id, 
                                    Muuttaa_Statement::STATUS_FAILED);
            } catch (Muuttaa_Exception_FailedQuery $e) {
                // A known failed query happend. We record that and then
                // attempt to run it again later if retries > 0.
                $sql = 'UPDATE ' . $this->getTable() . '
                        SET tries = (tries + 1)
                        WHERE id = ?';

                $this->db()->query($sql, array((int)$stmt->id));
                $this->logError($stmt, $e);
            } catch (Exception $e) {
                // An unknown fatal exception happened. This is seriously
                // not good so we mark the statement as totally failed 
                // immediately and disregard retries.
                $this->updateStatus($stmt->id, 
                                    Muuttaa_Statement::STATUS_EXCEPTION);
            
                $this->logError($stmt, $e);
            }
        }

        // Queue up a Muuttaa statement that will, in fact, prune the oldest
        // 1000 successful statements from the queue. How meta meta of us!
        $sql = 'SELECT MIN(id)
                FROM ' . $this->getTable() . '
                WHERE status = ?';

        $min = $this->db()->getOne($sql, array(
            Muuttaa_Statement::STATUS_COMPLETE
        ));

        $queue = new Muuttaa($this->name, $this->queue);
        if ((int)$min > 0) {
            $sql = 'DELETE FROM ' . $this->getTable() . ' 
                    WHERE id >= ' . $min .' AND
                          id <= ' . ($min + 1000) . ' AND
                          status = ' . Muuttaa_Statement::STATUS_COMPLETE;

            $stmt = new Muuttaa_Statement();
            $stmt->addQuery($sql);
            $stmt->addHost($this->queue);
            $queue->addStatement($stmt);
            $queue->commit();
        }

        // Nuke all errors that are older than 30 days. If you haven't fixed
        // your shit in 30 days you probably won't ever fix it. This probably
        // also means you're lazy, which is why we clean them up for you. We
        // only delete 1000 of them per pass though.
        $sql = 'SELECT id
                FROM ' . $this->getTable() . '
                WHERE status IN (?,?) AND 
                      date_created <= DATE_SUB(NOW(), INTERVAL 30 DAY)
                ORDER BY date_created DESC
                LIMIT 1000';

        $errors = $this->db->getCol($sql, 0, array(
            Muuttaa_Statement::STATUS_FAILED,
            Muuttaa_Statement::STATUS_EXCEPTION
        ));

        foreach ($errors as $err) {
            $stmt = new Muuttaa_Statement();
            $stmt->addQuery('DELETE 
                             FROM ' . $this->getTable() . '
                             WHERE id = ' . (int)$err);
            $stmt->addQuery('DELETE 
                             FROM ' . $this->getTable('errors') . '
                             WHERE id = ' . (int)$err);
            $stmt->addHost($this->queue);
            $queue->addStatement($stmt);
            $queue->commit();
        }

        $this->unlock();
    }

    /**
     * Get a user lock
     *
     * @return void
     * @throws {@link Muuttaa_Exception_FailedLock} on failed locks
     */
    protected function lock()
    {
        $res = $this->db()->getOne('SELECT GET_LOCK(?, 1)', array(
            $this->getLockName()
        ));

        if ((int)$res === 1) {
            return true;
        }

        throw new Muuttaa_Exception_FailedLock();
    }

    /**
     * Unlock the queue
     *
     * @return void
     * @throws {@link Muuttaa_Exception_FailedLock} on failed unlocks
     * @see Muuttaa_Process::getLockName()
     */
    protected function unlock()
    {
        $res = $this->db()->getOne('SELECT RELEASE_LOCK(?)', array(
            $this->getLockName()
        ));

        if ((int)$res === 1) {
            return true;
        }

        throw new Muuttaa_Exception_FailedLock();
    }

    /**
     * Get a lock name
     *
     * @see Muuttaa_Common::$name
     * @return string
     */
    protected function getLockName()
    {
        return sprintf('muuttaa_%s', $this->name);
    }

    /**
     * Run the {@link Muuttaa_Statement} 
     *
     * Takes a {@link Muuttaa_Statement} record and attempts to run it against
     * all hosts. If *any* of those fails it will roll back that one 
     * transaction and {@link Muuttaa_Process::process()} will mark it as
     * failed. The statement must run successfully across *all* of the hosts
     * in order for it to qualify as being successful.
     *
     * @param object $stmt A record from the queue to process
     *
     * @access protected
     * @return void
     * @throws {@link Muuttaa_Exception_FailedQuery} on failed queries
     */
    protected function runStatement($stmt)
    {
        $hosts   = json_decode($stmt->hosts);
        $queries = json_decode($stmt->statement);

        foreach ($hosts as $host) {
            $this->link($host)->beginTransaction();
            try {
                foreach ($queries as $sql) {
                    $this->link($host)->query($sql);
                }

                $this->link($host)->commit();
            } catch (PDB_Exception $e) {
                $this->link($host)->rollBack(); 
                throw new Muuttaa_Exception_FailedQuery(
                    $sql, $e->getMessage(), $e->getCode()
                );
            }
        }
    }

    /**
     * Log an error 
     *
     * All queues have a corresponding error log under the name of the queue
     * appended with '_errors'. The log holds the statementid that died, 
     * error code, error message, and the query (if any, as exceptions can
     * happen from non-SQL errors).
     *
     * @param object $stmt {@link Muuttaa_statement} that failed
     * @param object $e    The exception associated with the failure
     *
     * @access protected
     * @return void
     */
    protected function logError($stmt, Exception $e)
    {
        $query = '/** FATAL NON-QUERY ERROR **/';
        if (method_exists($e, 'getQuery')) {
            $query = $e->getQuery();
        }

        $sql = 'INSERT INTO ' . $this->getTable('errors') . '
                SET statementid = ?,
                    code = ?,
                    message = ?,
                    query = ?,
                    date_created = NOW()';

        $this->db()->query($sql, array(
            (int)$stmt->id, (int)$e->getCode(), $e->getMessage(), $query
        ));
    }

    /**
     * Update the {@link Muuttaa_Statement}'s status
     *
     * @param integer $id     The statement's unique ID
     * @param integer $status The new status
     *
     * @return true
     */
    protected function updateStatus($id, $status)
    {
        $sql = 'UPDATE ' . $this->getTable() . '
                SET status = ? 
                WHERE id = ?';

        $this->db()->query($sql, array((int)$status, (int)$id));
        return true;
    }

    /**
     * Establish a DB link to host
     *
     * This is used to establish a connection to the host on which the various
     * {@link Muuttaa_Statement}'s have specified they be ran. This should not
     * be confused with the connections to the actual queue itself.
     *
     * @param object $host A valid host to connect to
     *
     * @access protected
     * @return object Instance of {@link PDB}
     * @static
     */
    protected function link($host) 
    {
        if (!self::isValidHost($host)) {
            throw new InvalidArgumentException('Host is invalid');
        }

        $dsn = self::dsn($host);
        if (isset($this->links[$dsn])) {
            return $this->links[$dsn];
        }

        $this->links[$dsn] = PDB::connect($dsn, $host->user, $host->pass);
        return $this->links[$dsn];
    }

    /**
     * Destructor
     *
     * Close the connections to all of our database servers.
     *
     * @return void
     * @see Muuttaa_Process::$links
     */
    public function __destruct()
    {
        foreach ($this->links as $dsn => $conn) {
            $this->links[$dsn] = null;
        }
    }
}

?>
