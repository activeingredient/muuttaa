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
 * @copyright 2008 Digg.com, Inc. 
 * @license   http://tinyurl.com/42zef New BSD License
 * @version   SVN: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */

require_once 'Muuttaa/Exception.php';

/**
 * Failed to obtain a lock in {@link Muuttaa_Process::process()}
 *
 * @package   Muuttaa
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2008 Digg.com, Inc. 
 * @license   http://tinyurl.com/42zef New BSD License
 * @version   Release: @package_version@
 * @link      http://code.google.com/p/muuttaa
 */
class Muuttaa_Exception_FailedLock extends Muuttaa_Exception
{

}

?>
