<?php
/*
 * BSD 3-Clause License
 *
 * Copyright (c) 2021, TASoft Applications
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 *  Neither the name of the copyright holder nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

namespace Ikarus\SPS\Register;


use Ikarus\SPS\Alert\AlertInterface;
use Ikarus\SPS\Alert\CriticalAlert;
use Ikarus\SPS\Alert\NoticeAlert;
use Ikarus\SPS\Alert\WarningAlert;
use Ikarus\SPS\Exception\EngineControlException;
use Ikarus\SPS\Plugin\PluginInterface;
use Ikarus\SPS\Register\PDOFactory\PDOFactoryInterface;
use TASoft\Util\PDO;

class CommonPDOMemoryRegister implements CommonMemoryRegisterInterface, WorkflowDependentMemoryRegister
{
	/** @var PDOFactoryInterface */
	private $factory;
	/** @var string */
	private $identifier;
	/** @var PDO */
	private $_pdo;

	private $alerts = [];

	/**
	 * Each common memory register must specify a unique id that lets the common server distinguish between all Ikarus SPS instances.
	 *
	 * @param string $identifier
	 */
	public function __construct(PDOFactoryInterface $factory, string $identifier)
	{
		$this->identifier = $identifier;
		$this->factory = $factory;
	}

	private function getPDO(): PDO {
		if(!$this->_pdo) {
			$this->_pdo = $this->getFactory()->getPDO();

			$this->_pdo->exec("DELETE FROM STATUS_REGISTER WHERE reg_brick = '@SPS'; DELETE FROM COMMAND_REGISTER WHERE reg_command = '@SPS_STOP'; INSERT INTO STATUS_REGISTER (reg_brick, reg_status) VALUES ('@SPS', 1)");
		}
		return $this->_pdo;
	}

	/**
	 * @inheritDoc
	 */
	public function getAlerts(): array
	{
		$alrts = [];
		foreach($this->getPDO()->select("SELECT reg_id, id AS uid, code, message, brick, date, level FROM ALERT_REGISTER WHERE acknowledged IS NULL ORDER BY date DESC") as $record) {
			$date = new \DateTime( $record['date'] );
			$record['time'] = $date->format("G:i");
			$record['date'] = $date->format("d.m.Y");

			$alrts[] = $record;
		}
		return $alrts;
	}

	/**
	 * @inheritDoc
	 */
	public function getPendentAlertCount(): int
	{
		return $this->getPDO()->selectFieldValue("SELECT count(id) AS CNT FROM ALERT_REGISTER WHERE reg_id=? AND acknowledged IS NULL", 'CNT', [$this->getIdentifier()]) * 1;
	}

	/**
	 * @inheritDoc
	 */
	public function stopCycle(int $code = 0, string $reason = "")
	{
		// Not relevant for detached Ikarus SPS plugins, because they always are alone in a cycle.
	}

	/**
	 * @inheritDoc
	 */
	public function stopEngine(int $code = 0, string $reason = "")
	{
		$this->getPDO()->inject("INSERT INTO COMMAND_REGISTER (reg_command, reg_info) VALUES ('@SPS_STOP', ?)")->send([
			serialize([$code, $reason])
		]);
	}

	/**
	 * @inheritDoc
	 */
	public function putCommand(string $command, $info = NULL)
	{
		$command = $this->getPDO()->quote($command);
		$info = $this->getPDO()->quote( serialize($info) );

		$this->getPDO()->exec("DELETE FROM COMMAND_REGISTER WHERE reg_command = $command; INSERT INTO COMMAND_REGISTER (reg_command, reg_info) VALUES ($command, $info)");
	}

	/**
	 * @inheritDoc
	 */
	public function hasCommand(string $command = NULL): bool
	{
		return $this->getPDO()->selectFieldValue("SELECT count(reg_command) AS C FROM COMMAND_REGISTER WHERE reg_command = ?", 'C', [$command]) ? true : false;
	}

	/**
	 * @inheritDoc
	 */
	public function getCommand(string $command)
	{
		$c = $this->getPDO()->selectFieldValue("SELECT reg_info FROM COMMAND_REGISTER WHERE reg_command = ?", 'reg_info', [$command]);
		return $c ? unserialize($c) : NULL;
	}

	/**
	 * @inheritDoc
	 */
	public function clearCommand(string $command = NULL)
	{
		if($command)
			$this->getPDO()->inject("DELETE FROM COMMAND_REGISTER WHERE reg_command = ?")->send([$command]);
		else
			$this->getPDO()->exec("DELETE FROM COMMAND_REGISTER");
	}

	/**
	 * @inheritDoc
	 */
	public function putValue($value, string $key, string $domain)
	{
		$value = $this->getPDO()->quote( serialize($value) );
		$key = $this->getPDO()->quote($key);
		$domain = $this->getPDO()->quote($domain);

		$this->getPDO()->exec("DELETE FROM VALUE_REGISTER WHERE reg_key = $key AND reg_domain = $domain;
INSERT INTO VALUE_REGISTER (reg_key, reg_domain, reg_data) VALUES ($key, $domain, $value)");
	}

	/**
	 * @inheritDoc
	 */
	public function hasValue(string $domain, $key = NULL): bool
	{
		if($key)
			return $this->getPDO()->selectFieldValue("SELECT count(reg_key) AS C FROM VALUE_REGISTER WHERE reg_domain = ? AND reg_key = ?", 'C', [$domain, $key]) ? true : false;
		return $this->getPDO()->selectFieldValue("SELECT count(reg_key) AS C FROM VALUE_REGISTER WHERE reg_domain = ?", 'C', [$domain]) ? true : false;
	}

	/**
	 * @inheritDoc
	 */
	public function fetchValue(string $domain, $key = NULL)
	{
		if($key) {
			$value = $this->getPDO()->selectFieldValue("SELECT reg_data FROM VALUE_REGISTER WHERE reg_key = ? AND reg_domain = ?", 'reg_data', [$key, $domain]);
			return $value ? unserialize( $value ) : NULL;
		}

		$values = [];
		foreach($this->getPDO()->select("SELECT reg_data, reg_key FROM VALUE_REGISTER WHERE reg_domain = ?", [$domain]) as $record) {
			$values[ $record['reg_key'] ] = $record["reg_data"] ? unserialize( $record["reg_data"] ) : NULL;
		}
		return $values;
	}

	/**
	 * @inheritDoc
	 */
	public function setStatus(int $status, string $pluginID)
	{
		$pluginID = $this->getPDO()->quote($pluginID);
		$this->getPDO()->exec("DELETE FROM STATUS_REGISTER WHERE reg_brick = $pluginID; INSERT INTO STATUS_REGISTER (reg_brick, reg_status) VALUES ($pluginID, $status)");
	}

	public function getStatus(string $pluginID): ?int
	{
		$s = $this->getPDO()->selectFieldValue("SELECT reg_status FROM STATUS_REGISTER WHERE reg_brick = ?", 'reg_status', [$pluginID]);
		return NULL !== $s ? $s*1: NULL;
	}

	public function triggerAlert(AlertInterface $alert)
	{
		$pl = $alert->getAffectedPlugin();

		$level = 3;
		if($alert instanceof NoticeAlert)
			$level = 1;
		elseif($alert instanceof WarningAlert)
			$level = 2;

		$this->getPDO()->inject("INSERT INTO ALERT_REGISTER (date, code, level, message, brick, reg_id) VALUES (?,?,?,?,?,?)")->send([
			(new \DateTime())->format("Y-m-d G:i:s"),
			$alert->getCode(),
			$level,
			$alert->getMessage(),
			$pl instanceof PluginInterface ? $pl->getIdentifier() : "",
			$this->getIdentifier()
		]);
		$alert->setID( $aid = $this->getPDO()->lastInsertId("ALERT_REGISTER") );
		$this->alerts[$aid] = $alert;
	}

	public function acknowledgeAlert(int $alertID): bool
	{
		$date = (new \DateTime())->format("Y-m-d G:i:s");
		return false !== $this->getPDO()->exec("UPDATE ALERT_REGISTER SET acknowledged = '$date' WHERE id = $alertID");
	}

	public function setup()
	{
	}

	public function tearDown()
	{
		$this->getPDO()->exec("UPDATE STATUS_REGISTER SET reg_status = 0 WHERE reg_brick = '@SPS'");
	}

	public function beginCycle()
	{
		if(IKARUS_MAIN_PROCESS) {
			if($info = $this->getCommand("@SPS_STOP")) {
				throw (new EngineControlException($info[1], $info[0]))->setControl( EngineControlException::CONTROL_STOP_ENGINE );
			}
		}

		if($this->alerts) {
			$aFilter = implode(",", array_keys($this->alerts));
			foreach($this->getPDO()->select("SELECT id FROM ALERT_REGISTER WHERE id IN ($aFilter) AND acknowledged IS NOT NULL") as $record) {
				$alert = $this->alerts[ $record['id'] ];
				if($alert instanceof CriticalAlert) {
					if(is_callable( $cb = $alert->getCallback() ))
						call_user_func($cb);
				}
				unset($this->alerts[ $record['id'] ]);
			}
		}
	}

	public function endCycle()
	{
	}

	/**
	 * @return PDOFactoryInterface
	 */
	public function getFactory(): PDOFactoryInterface
	{
		return $this->factory;
	}

	/**
	 * @return string
	 */
	public function getIdentifier(): string
	{
		return $this->identifier;
	}
}