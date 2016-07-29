package org.apache.spark.storage

import java.io.IOException
import java.nio.ByteBuffer

import java.net.InetAddress
import java.net.NetworkInterface

import scala.util.control.NonFatal
import scala.util.Random

import com.google.common.io.ByteStreams

import edu.cuhk.cse.memec.MemEC

import org.apache.spark.Logging
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}

import java.security.MessageDigest

private[spark] class MemECBlockManager() extends ExternalBlockManager with Logging {
	private var memecs: Array[MemEC] = _
	private var current: Int = 0
	private var keyPrefix: String = _

	def checkIsMyIP( ip: String ): Boolean = {
		val addr = InetAddress.getByName( ip )
		if ( addr.isAnyLocalAddress() || addr.isLoopbackAddress() )
			return true
		// Check if the address is defined on any interface
		try {
			return NetworkInterface.getByInetAddress( addr ) != null
		} catch {
			case e: Exception => return false
		}
	}

	override def init( blockManager: BlockManager, executorId: String ): Unit = {
		super.init( blockManager, executorId )
		val hosts = blockManager.conf.get( "spark.MemEC.hosts" ).split( "," )
		val ports = blockManager.conf.get( "spark.MemEC.ports" ).split( "," ).map( _.toInt )
		val replicate = blockManager.conf.get( "spark.MemEC.replicate", "1" ).toInt
		val keySize = blockManager.conf.get( "spark.MemEC.keySize", MemEC.DEFAULT_KEY_SIZE.toString() ).toInt
		val chunkSize = blockManager.conf.get( "spark.MemEC.chunkSize", MemEC.DEFAULT_CHUNK_SIZE.toString() ).toInt

		val r = Random
		var fromId = r.nextInt
		var toId = r.nextInt
		if ( fromId > toId ) {
			val tmp = fromId
			fromId = toId
			toId = tmp
		}

		if ( hosts.length != ports.length ) {
			logError( "The number of hosts is not equal to the number of ports" )
			throw new IOException( "The number of hosts is not equal to the number of ports" )
		}

		var count = 0
		for ( i <- 0 until hosts.length ) {
			val isMyIP: Boolean = this.checkIsMyIP( hosts( i ) )
			if ( isMyIP )
				count += 1
			logInfo( s"Host #${i}: ${hosts( i )}: " + ( if ( isMyIP ) "is my IP" else "-" ) )
		}

		if ( count == 0 ) {
			count = hosts.length
			this.memecs = new Array[ MemEC ]( count * replicate )
			var j = 0
			for ( r <- 1 to replicate ) {
				for ( i <- 0 until hosts.length ) {
					this.memecs( j ) = new MemEC( keySize, chunkSize, hosts( i ), ports( i ), fromId, toId )
					j += 1
				}
			}
		} else {
			this.memecs = new Array[ MemEC ]( count * replicate )
			var j = 0
			for ( r <- 1 to replicate ) {
				for ( i <- 0 until hosts.length ) {
					val isMyIP = checkIsMyIP( hosts( i ) )
					if ( isMyIP ) {
						this.memecs( j ) = new MemEC( keySize, chunkSize, hosts( i ), ports( i ), fromId, toId )
						logInfo( s"Connecting to host #${j}: ${hosts( i )}:${ports( i )}" )
						j += 1
					}
				}
			}
		}

		for ( i <- 0 until this.memecs.length ) {
			if ( ! this.memecs( i ).connect() ) {
				logError( "Failed to connect to the MemEC client" )
				throw new IOException( "Failed to connect to the MemEC client" )
			}
		}

		val basePrefix = blockManager.conf.get( "spark.MemEC.keyPrefix", "spark" )
		val appPrefix = blockManager.conf.get( ExternalBlockStore.FOLD_NAME )
		this.keyPrefix = s"$basePrefix:$appPrefix:$executorId"
	}

	override def toString: String = { "ExternalBlockStore-MemEC" }

	override def shutdown() {
		logInfo( "MemECBlockManager: Shutdown hook called" )
		for ( i <- 0 until this.memecs.length )
			this.memecs( i ).disconnect()
	}

	def getKey( blockId: BlockId ): String = {
		s"${this.keyPrefix}:${blockId.name}"
	}

	def getMemEC(): MemEC = {
		val ret = this.memecs( this.current )
		this.current += 1
		if ( this.current == this.memecs.length )
			this.current = 0
		return ret
	}

	override def removeBlock( blockId: BlockId ): Boolean = {
		val memec = this.getMemEC()
		memec.synchronized {
			return memec.delete( getKey( blockId ) )
		}
	}

	override def blockExists( blockId: BlockId ): Boolean = {
		val memec = this.getMemEC()
		memec.synchronized {
			return memec.get( getKey( blockId ) ) != null
		}
	}

	def md5( b: Array[Byte] ) = {
		MessageDigest.getInstance( "MD5" ).digest( b )
	}

	override def putBytes( blockId: BlockId, bytes: ByteBuffer ): Unit = {
		val key = getKey( blockId ).getBytes
		val value = bytes.array
		// val hash = md5( value )
		// logInfo( s"putBytes(): ${getKey(blockId)} - ${hash(0)} ${hash(1)} ${hash(2)} ${hash(3)} ${hash(4)} (value size = ${value.length})" )
		val memec = this.getMemEC()
		memec.synchronized {
			memec.set( key, key.length, value, value.length )
		}
	}

	override def getBytes( blockId: BlockId ): Option[ ByteBuffer ] = {
		val key = getKey( blockId ).getBytes
		val memec = this.getMemEC()
		memec.synchronized {
			val value = memec.getRaw( key, key.length )
			// val hash = md5( value )
			// logInfo( s"getBytes(): ${getKey(blockId)} - ${hash(0)} ${hash(1)} ${hash(2)} ${hash(3)} ${hash(4)} (value size = ${value.length})" )
			if ( value == null )
				return None
			return Some( ByteBuffer.wrap( value ) )
		}
	}

	override def getSize( blockId: BlockId ): Long = {
		val key = getKey( blockId ).getBytes
		val memec = this.getMemEC()
		memec.synchronized {
			val value = memec.getRaw( key, key.length )
			return value.length
		}
	}
}
