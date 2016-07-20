package org.apache.spark.storage

import java.io.IOException
import java.nio.ByteBuffer

import scala.util.control.NonFatal
import scala.util.Random

import com.google.common.io.ByteStreams

import edu.cuhk.cse.memec.MemEC

import org.apache.spark.Logging
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}

import java.security.MessageDigest

private[spark] class MemECBlockManager() extends ExternalBlockManager with Logging {
	private var memec: MemEC = _
	private var keyPrefix: String = _

	override def init( blockManager: BlockManager, executorId: String ): Unit = {
		super.init( blockManager, executorId )
		val host = blockManager.conf.get( "spark.MemEC.host" )
		val port = blockManager.conf.get( "spark.MemEC.port", MemEC.DEFAULT_PORT.toString() ).toInt
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

		this.memec = new MemEC( keySize, chunkSize, host, port, fromId, toId )

		if ( ! this.memec.connect() ) {
			logError( "Failed to connect to the MemEC client" )
			throw new IOException( "Failed to connect to the MemEC client" )
		}

		val basePrefix = blockManager.conf.get( "spark.MemEC.keyPrefix", "spark" )
		val appPrefix = blockManager.conf.get( ExternalBlockStore.FOLD_NAME )
		this.keyPrefix = s"$basePrefix:$appPrefix:$executorId"
	}

	override def toString: String = { "ExternalBlockStore-MemEC" }

	override def shutdown() {
		logInfo( "MemECBlockManager: Shutdown hook called" )
		memec.disconnect()
	}

	def getKey( blockId: BlockId ): String = {
		s"${this.keyPrefix}:${blockId.name}"
	}

	override def removeBlock( blockId: BlockId ): Boolean = {
		return memec.delete( getKey( blockId ) )
	}

	override def blockExists( blockId: BlockId ): Boolean = {
		return memec.get( getKey( blockId ) ) != null
	}

	override def putBytes( blockId: BlockId, bytes: ByteBuffer ): Unit = {
		val key = getKey( blockId ).getBytes
		val value = bytes.array
		memec.set( key, key.length, value, value.length )
	}

	override def getBytes( blockId: BlockId ): Option[ ByteBuffer ] = {
		val key = getKey( blockId ).getBytes
		val value = memec.getRaw( key, key.length )
		if ( value == null )
			return None
		return Some( ByteBuffer.wrap( value ) )
	}

	override def getSize( blockId: BlockId ): Long = {
		val ret = memec.get( getKey( blockId ) )
		return ret.length
	}
}
