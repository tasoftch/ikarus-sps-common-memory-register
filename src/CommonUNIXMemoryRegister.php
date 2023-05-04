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

use Ikarus\SPS\Register\Exception\SocketConnectionException;

class CommonUNIXMemoryRegister extends AbstractCommonMasterMemoryRegister
{
	/** @var string */
	private $unixSocketName;


	public function __construct($identifier, string $unixName, bool $master, bool $useWS)
	{
		parent::__construct($identifier, $master, $useWS);
		$this->unixSocketName = $unixName;
	}



	/**
	 * @return string
	 */
	public function getUnixSocketName(): string
	{
		return $this->unixSocketName;
	}

	/**
	 * @param string $unixSocketName
	 * @return static
	 */
	public function setUnixSocketName(string $unixSocketName)
	{
		$this->unixSocketName = $unixSocketName;
		return $this;
	}
	/**
	 * @inheritDoc
	 */
	protected function connectionType(): string
	{
		return static::SERVER_TYPE_UNIX;
	}
	/**
	 * @inheritDoc
	 */
	protected function connectionAddress(): string
	{
		return $this->getUnixSocketName();
	}
	/**
	 * @inheritDoc
	 */
	protected function connectionPort(): int
	{
		return 0;
	}


	/**
	 * @inheritDoc
	 */
	protected function connectSocket()
	{
		$this->socket = socket_create(AF_UNIX, SOCK_STREAM, 0);
		if(!$this->socket)
			throw (new SocketConnectionException("Can not create socket of type AF_UNIX", 889));

		if(!@socket_connect($this->socket, $n = $this->getUnixSocketName())) {
			$e = socket_last_error();
			$str = socket_strerror($e);
			throw (new SocketConnectionException("Can not connect to %s => ($e): $str", 890, NULL, $this->getUnixSocketName()))->setSocket($this->socket);
		}
	}

	/**
	 * @inheritDoc
	 */
	protected function disconnectSocket()
	{
		@socket_write($this->socket, 'exit', 4);
		@socket_close($this->socket);
	}
}