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
use Ikarus\SPS\EngineDependencyInterface;
use Ikarus\SPS\EngineInterface;
use Ikarus\SPS\Exception\EngineControlException;
use Ikarus\SPS\Plugin\PluginInterface;

abstract class AbstractCommonMemoryRegister implements CommonMemoryRegisterInterface, WorkflowDependentMemoryRegister, EngineDependencyInterface
{
	const SOCKET_BUFFER_SIZE = 8192;

	/** @var string */
	private $identifier;
	/** @var resource */
	protected $socket;

	protected $alerts = [];

	private $pendentAlertCount = 0;

	/** @var EngineInterface */
	private $engine ;

	public function setEngine(?EngineInterface $engine)
	{
		$this->engine = $engine;
	}


	/**
	 * AbstractCommonMemoryRegister constructor.
	 *
	 * Each common memory register must specify a unique id that lets the common server distinguish between all Ikarus SPS instances.
	 *
	 * @param string $identifier
	 */
	public function __construct(string $identifier)
	{
		$this->identifier = $identifier;
	}

	/**
	 * @return string
	 */
	public function getIdentifier(): string
	{
		return $this->identifier;
	}

	/**
	 * This method must establish the socket connection to the server
	 * @return resource|false
	 */
	abstract protected function connectSocket();

	/**
	 * Disconnects the connection to the common server
	 */
	abstract protected function disconnectSocket();

	/**
	 * Sends a command to the server and waits the server's response.
	 *
	 * @param $command
	 */
	protected function sendCommand($command) {
		if(NULL === $this->socket) {
			$this->connectSocket();
			if(!is_resource( $this->socket ))
				$this->socket = false;
		}
		if($this->socket) {
			socket_clear_error($this->socket);
			while ($len = @socket_write($this->socket, $command, static::SOCKET_BUFFER_SIZE)) {
				if($len == ($strlen = min(static::SOCKET_BUFFER_SIZE, strlen($command))))
					break;

				$command = substr($command, $strlen);
			}
			if($e = socket_last_error($this->socket)) {
				error_clear_last();
				throw new \RuntimeException("Memory register server has gone. " . socket_strerror($e));
			}

			$response = "";
			while ($out = socket_read($this->socket, static::SOCKET_BUFFER_SIZE)) {
				$response.=$out;
				if(strlen($out) < static::SOCKET_BUFFER_SIZE)
					break;
			}
			return @ unserialize( $response );
		} else
			trigger_error("No connection to server provided", E_USER_ERROR);
	}

	public function stopCycle(int $code = 0, string $reason = "")
	{
		// Not relevant for detached Ikarus SPS plugins, because they always are alone in a cycle.
	}

	/**
	 * @inheritDoc
	 */
	public function stopEngine(int $code = 0, string $reason = "")
	{
		return $this->sendCommand("stop " . serialize([$code, $reason])) ? true : false;
	}

	/**
	 * @inheritDoc
	 */
	public function putCommand(string $command, $info = NULL)
	{
		return $this->sendCommand("putc " . serialize([$command, $info])) ? true : false;
	}

	/**
	 * @inheritDoc
	 */
	public function hasCommand(string $command = NULL): bool
	{
		return $this->sendCommand("hasc " . serialize([$command])) ? true : false;
	}

	/**
	 * @inheritDoc
	 */
	public function getCommand(string $command)
	{
		return unserialize( $this->sendCommand("getc " . serialize([$command])) );
	}

	/**
	 * @inheritDoc
	 */
	public function clearCommand(string $command = NULL)
	{
		return $this->sendCommand("clearc " . serialize([$command])) ? true : false;
	}

	/**
	 * @inheritDoc
	 */
	public function putValue($value, $key, $domain, bool $merge = false)
	{
		return $this->sendCommand("putv " . serialize([$value, $key, $domain, $merge])) ? true : false;
	}

	/**
	 * @inheritDoc
	 */
	public function hasValue($domain, $key = NULL): bool
	{
		return $this->sendCommand("hasv " . serialize([$domain, $key])) ? true : false;
	}

	/**
	 * @inheritDoc
	 */
	public function fetchValue($domain, $key = NULL)
	{
		return $this->sendCommand("getv " . serialize([$domain, $key]));
	}

	/**
	 * @inheritDoc
	 */
	public function setStatus(int $status, string $pluginID)
	{
		return $this->sendCommand("puts " . serialize([$status, $pluginID])) ? true : false;
	}

	/**
	 * @inheritDoc
	 */
	public function getStatus(string $pluginID): ?int
	{
		return $this->sendCommand("gets " . serialize([$pluginID])) * 1;
	}

	/**
	 * @inheritDoc
	 */
	public function triggerAlert(AlertInterface $alert)
	{
		$pl = $alert->getAffectedPlugin();

		$level = 3;
		if($alert instanceof NoticeAlert)
			$level = 1;
		elseif($alert instanceof WarningAlert)
			$level = 2;

		$info = [
			$this->identifier,
			$alert->getID(),
			$alert->getCode(),
			$alert->getMessage(),
			$alert->getTimeStamp(),
			$pl instanceof PluginInterface ? $pl->getIdentifier() : $pl,
			$level
		];

		$aid = $this->sendCommand("alrt " . serialize($info)) * 1;
		if(0 == $alert->getID())
			$alert->setID( $aid );

		$this->alerts[ $alert->getID() ] = $alert;
		$this->engine->getAlertManager()->dispatchAlert(
			$alert->getID(),
			$alert->getCode(),
			$alert->getLevel(),
			$alert->getMessage(),
			$alert->getAffectedPlugin() instanceof PluginInterface ? $alert->getAffectedPlugin()->getIdentifier() : $alert->getAffectedPlugin(),
			$alert->getTimeStamp()
		);
	}

	/**
	 * @inheritDoc
	 */
	public function acknowledgeAlert(int $alertID): bool
	{
		// Theoretically not used by detached plugins. But they are allowed to acknowledge alerts too.
		return $this->sendCommand("alrtq ".serialize([$alertID])) ? true : false;
	}

	/**
	 * Checks, if there are acknowledged alerts on the common register, which affect the detached plugin.
	 */
	public function beginCycle() {
		if($GLOBALS['IKARUS_MAIN_PROCESS']) {
			if($stop = $this->sendCommand('stopped ' . serialize([]))) {
				throw (new EngineControlException($stop[1], $stop[0]))->setControl( EngineControlException::CONTROL_STOP_ENGINE );
			}
		}
		if($this->alerts) {
			if($recovered = $this->sendCommand("ialrtq " . serialize([]))) {
				/** @var AlertInterface $alert */
				$alerts = $this->alerts;
				foreach($alerts as $alert) {
					if(!in_array("$this->identifier::".$alert->getID(), $recovered)) {
						if($alert instanceof CriticalAlert) {
							if(is_callable($alert->getCallback()))
								call_user_func($alert->getCallback());
						}
						unset($this->alerts[$alert->getID()]);
					}
				}
				unset($recovered["#"]);
				$this->pendentAlertCount = count($recovered);
			}
		}
		else
			$this->pendentAlertCount = 0;
	}

	/**
	 * not used, but implemented to conform the interface.
	 */
	public function setup()
	{
	}

	/**
	 * Gets called by ikarus/sps version 1.1 and above
	 */
	public function tearDown() {
		if($this->socket)
			$this->disconnectSocket();
	}

	/**
	 * not used, but implemented to conform the interface.
	 */
	public function endCycle()
	{
		$this->sendCommand("endcycle ".serialize([]));
	}

	/**
	 * @return array
	 */
	public function getAlerts(): array
	{
		$alrts = [];
		if($alerts = $this->sendCommand("alrtget " . serialize([]))) {
			foreach($alerts as $key => $value) {
				if($key == '#')
					continue;

				list($code, $msg, $ts, $plugin, $level) = $value;
				list($project, $key) = explode("::", $key, 2);

				$alrts[] = [
					"reg_id" => $project,
					'uid' => $key,
					'code' => $code,
					'message' => $msg,
					'brick' => $plugin,
					'date' => date("d.m.Y", $ts),
					"time" => date("G:i", $ts),
					'level' => $level
				];
			}
		}
		return $alrts;
	}

	/**
	 * @return int
	 */
	public function getPendentAlertCount(): int
	{
		return $this->pendentAlertCount;
	}
}