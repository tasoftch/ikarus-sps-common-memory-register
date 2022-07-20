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


use Ikarus\SPS\Register\Exception\CommonMemoryRegisterException;
use TASoft\Util\BackgroundProcess;

abstract class AbstractCommonMasterMemoryRegister extends AbstractCommonMemoryRegister
{
	const SERVER_TYPE_UNIX = 'unix';
	const SERVER_TYPE_TCP = 'inet';

	/** @var bool */
	private $master, $ws;
	/** @var BackgroundProcess|null */
	private $process;

	public function __construct(string $identifier, bool $master, bool $useWS = false)
	{
		parent::__construct($identifier);
		$this->master = $master;
		$this->ws = $useWS;
	}

	/**
	 * Specifies the type of how the server needs to boot (if is master)
	 *
	 * @return string
	 */
	abstract protected function connectionType(): string;

	/**
	 * Specifies the ip address or socket name to launch the server
	 *
	 * @return string
	 */
	abstract protected function connectionAddress(): string;

	/**
	 * Specified the tcp port if the server launches as tcp server.
	 * @return int
	 */
	abstract protected function connectionPort(): int;

	/**
	 * Pass more arguments to the server process
	 *
	 * @return array|null
	 */
	protected function getAdditionalServerArguments(): ?array {
		if(IKARUS_VISUAL_CONN_MODE == 'ws' && IKARUS_VISUAL_INTERFACE)
			return ['--hook-server', escapeshellarg( IKARUS_VISUAL_INTERFACE )];
		return NULL;
	}

	/**
	 * @return bool
	 */
	public function isMaster(): bool
	{
		return $this->master;
	}

	/**
	 * Setup the required server instance if the register is master.
	 */
	public function setup()
	{
		parent::setup();
		if($this->isMaster()) {
			$this->process = new BackgroundProcess("ikarus-sps mreg:default");
			$this->process->run();
		}
	}

	/**
	 * Tears down the launched server if the register was master.
	 */
	public function tearDown()
	{
		if($this->isMaster() && $this->process) {
			$this->process->kill( SIGTERM );
		}
		parent::tearDown();
	}
}