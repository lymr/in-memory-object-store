package com.lymr.memos

import com.typesafe.config.ConfigFactory

object Configuration {

  // -- Configuration Keys --- //

  private val DEFAULT_BLOCK_SIZE_KEY: String = "memos.default-block-size"

  // --- Private Members --- //

  private lazy val config = ConfigFactory.load()

  // --- Public Configuration --- //

  /** Default block size in Bytes */
  def defaultBlockSize: Int = config.getInt(DEFAULT_BLOCK_SIZE_KEY)
}
