package org.apache.hadoop.mapreduce.lib.input;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.io.IOException;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TextInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> 
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
    String delimiter = context.getConfiguration().get(
        "textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter)
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    return new LineRecordReader(recordDelimiterBytes);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec =
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }



/**
 * Treats keys as offset in file and value as line. 
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class LineRecordReader extends RecordReader<LongWritable, Text> {
  //private static final Log LOG = LogFactory.getLog(LineRecordReader.class);
  public static final String MAX_LINE_LENGTH = 
    "mapreduce.input.linerecordreader.line.maxlength";

  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private FSDataInputStream fileIn;
  private Seekable filePosition;
  private int maxLineLength;
  private LongWritable key;
  private Text value;
  private boolean isCompressedInput;
  private Decompressor decompressor;
  private byte[] recordDelimiterBytes;

  public LineRecordReader() {
  }

  public LineRecordReader(byte[] recordDelimiter) {
    this.recordDelimiterBytes = recordDelimiter;
  }

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);
    
    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (null!=codec) {
      isCompressedInput = true;	
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        final SplitCompressionInputStream cIn =
          ((SplittableCompressionCodec)codec).createInputStream(
            fileIn, decompressor, start, end,
            SplittableCompressionCodec.READ_MODE.BYBLOCK);
        if (null == this.recordDelimiterBytes){
          in = new LineReader(cIn, job);
        } else {
          in = new LineReader(cIn, job, this.recordDelimiterBytes);
        }

        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        filePosition = cIn;
      } else {
        if (null == this.recordDelimiterBytes) {
          in = new LineReader(codec.createInputStream(fileIn, decompressor),
              job);
        } else {
          in = new LineReader(codec.createInputStream(fileIn,
              decompressor), job, this.recordDelimiterBytes);
        }
        filePosition = fileIn;
      }
    } else {
      fileIn.seek(start);
      if (null == this.recordDelimiterBytes){
        in = new LineReader(fileIn, job);
      } else {
        in = new LineReader(fileIn, job, this.recordDelimiterBytes);
      }

      filePosition = fileIn;
    }
    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    if (start != 0) {
      start += in.readLine(new Text(), 0, maxBytesToConsume(start));
    }
    this.pos = start;
  }
  

  private int maxBytesToConsume(long pos) {
    return isCompressedInput
      ? Integer.MAX_VALUE
      : (int) Math.min(Integer.MAX_VALUE, end - pos);
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    while (getFilePosition() <= end) {
      newSize = in.readLine(value, maxLineLength,
          Math.max(maxBytesToConsume(pos), maxLineLength));
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }

      // line too long. try again
      //LOG.info("Skipped line of size " + newSize + " at pos " + 
        //       (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }
}


/**
 * A class that provides a line reader from an input stream.
 * Depending on the constructor used, lines will either be terminated by:
 * <ul>
 * <li>one of the following: '\n' (LF) , '\r' (CR),
 * or '\r\n' (CR+LF).</li>
 * <li><em>or</em>, a custom byte sequence delimiter</li>
 * </ul>
 * In both cases, EOF also terminates an otherwise unterminated
 * line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class LineReader implements Closeable {
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private InputStream in;
  private byte[] buffer;
  // the number of bytes of real data in the buffer
  private int bufferLength = 0;
  // the current position in the buffer
  private int bufferPosn = 0;

  private static final byte CR = '\r';
  private static final byte LF = '\n';

  // The line delimiter
  private final byte[] recordDelimiterBytes;

  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size (64k).
   * @param in The input stream
   * @throws IOException
   */
  public LineReader(InputStream in) {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a line reader that reads from the given stream using the 
   * given buffer-size.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @throws IOException
   */
		public LineReader(InputStream in, int bufferSize) {

			
			  MemcachedClient c = null; 
			  try 
			  { c = new MemcachedClient(new
			  BinaryConnectionFactory(), AddrUtil.getAddresses("127.0.0.1:11211"));} 
			  catch (Exception e) 
			  { System.out.println("no connect"); }
			  Object mystored = c.get("okk");
				String a = mystored.toString();
				InputStream is = new ByteArrayInputStream(a.getBytes());
			
			this.in = is;
			this.bufferSize = bufferSize;
			this.buffer = new byte[this.bufferSize];
			this.recordDelimiterBytes = null;

		}

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>.
   * @param in input stream
   * @param conf configuration
   * @throws IOException
   */
  public LineReader(InputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
  }

  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size, and using a custom delimiter of array of
   * bytes.
   * @param in The input stream
   * @param recordDelimiterBytes The delimiter
   */
  public LineReader(InputStream in, byte[] recordDelimiterBytes) {
    this.in = in;
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.buffer = new byte[this.bufferSize];
    this.recordDelimiterBytes = recordDelimiterBytes;
  }

  /**
   * Create a line reader that reads from the given stream using the
   * given buffer-size, and using a custom delimiter of array of
   * bytes.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @param recordDelimiterBytes The delimiter
   * @throws IOException
   */
  public LineReader(InputStream in, int bufferSize,
      byte[] recordDelimiterBytes) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
    this.recordDelimiterBytes = recordDelimiterBytes;
  }

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>, and using a custom delimiter of array of
   * bytes.
   * @param in input stream
   * @param conf configuration
   * @param recordDelimiterBytes The delimiter
   * @throws IOException
   */
  public LineReader(InputStream in, Configuration conf,
      byte[] recordDelimiterBytes) throws IOException {
    this.in = in;
    this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
    this.buffer = new byte[this.bufferSize];
    this.recordDelimiterBytes = recordDelimiterBytes;
  }


  /**
   * Close the underlying stream.
   * @throws IOException
   */
  public void close() throws IOException {
    in.close();
  }
  
  /**
   * Read one line from the InputStream into the given Text.
   *
   * @param str the object to store the given line (without newline)
   * @param maxLineLength the maximum number of bytes to store into str;
   *  the rest of the line is silently discarded.
   * @param maxBytesToConsume the maximum number of bytes to consume
   *  in this call.  This is only a hint, because if the line cross
   *  this threshold, we allow it to happen.  It can overshoot
   *  potentially by as much as one buffer length.
   *
   * @return the number of bytes read including the (longest) newline
   * found.
   *
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str, int maxLineLength,
                      int maxBytesToConsume) throws IOException {
    if (this.recordDelimiterBytes != null) {
      return readCustomLine(str, maxLineLength, maxBytesToConsume);
    } else {
      return readDefaultLine(str, maxLineLength, maxBytesToConsume);
    }
  }

  /**
   * Read a line terminated by one of CR, LF, or CRLF.
   */
  private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume)
  throws IOException {
    /* We're reading data from in, but the head of the stream may be
     * already buffered in buffer, so we have several cases:
     * 1. No newline characters are in the buffer, so we need to copy
     *    everything and read another buffer from the stream.
     * 2. An unambiguously terminated line is in buffer, so we just
     *    copy to str.
     * 3. Ambiguously terminated line is in buffer, i.e. buffer ends
     *    in CR.  In this case we copy everything up to CR to str, but
     *    we also need to see what follows CR: if it's LF, then we
     *    need consume LF as well, so next call to readLine will read
     *    from after that.
     * We use a flag prevCharCR to signal if previous character was CR
     * and, if it happens to be at the end of the buffer, delay
     * consuming it until we have a chance to look at the char that
     * follows.
     */
    str.clear();
    int txtLength = 0; //tracks str.getLength(), as an optimization
    int newlineLength = 0; //length of terminating newline
    boolean prevCharCR = false; //true of prev char was CR
    long bytesConsumed = 0;
    do {
      int startPosn = bufferPosn; //starting from where we left off the last time
      if (bufferPosn >= bufferLength) {
        startPosn = bufferPosn = 0;
        if (prevCharCR) {
          ++bytesConsumed; //account for CR from previous read
        }
        bufferLength = in.read(buffer);
        if (bufferLength <= 0) {
          break; // EOF
        }
      }
      for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
        if (buffer[bufferPosn] == LF) {
          newlineLength = (prevCharCR) ? 2 : 1;
          ++bufferPosn; // at next invocation proceed from following byte
          break;
        }
        if (prevCharCR) { //CR + notLF, we are at notLF
          newlineLength = 1;
          break;
        }
        prevCharCR = (buffer[bufferPosn] == CR);
      }
      int readLength = bufferPosn - startPosn;
      if (prevCharCR && newlineLength == 0) {
        --readLength; //CR at the end of the buffer
      }
      bytesConsumed += readLength;
      int appendLength = readLength - newlineLength;
      if (appendLength > maxLineLength - txtLength) {
        appendLength = maxLineLength - txtLength;
      }
      if (appendLength > 0) {
        str.append(buffer, startPosn, appendLength);
        txtLength += appendLength;
      }
    } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

    if (bytesConsumed > (long)Integer.MAX_VALUE) {
      throw new IOException("Too many bytes before newline: " + bytesConsumed);
    }
    return (int)bytesConsumed;
  }

  /**
   * Read a line terminated by a custom delimiter.
   */
  private int readCustomLine(Text str, int maxLineLength, int maxBytesToConsume)
      throws IOException {
   /* We're reading data from inputStream, but the head of the stream may be
    *  already captured in the previous buffer, so we have several cases:
    * 
    * 1. The buffer tail does not contain any character sequence which
    *    matches with the head of delimiter. We count it as a 
    *    ambiguous byte count = 0
    *    
    * 2. The buffer tail contains a X number of characters,
    *    that forms a sequence, which matches with the
    *    head of delimiter. We count ambiguous byte count = X
    *    
    *    // ***  eg: A segment of input file is as follows
    *    
    *    " record 1792: I found this bug very interesting and
    *     I have completely read about it. record 1793: This bug
    *     can be solved easily record 1794: This ." 
    *    
    *    delimiter = "record";
    *        
    *    supposing:- String at the end of buffer =
    *    "I found this bug very interesting and I have completely re"
    *    There for next buffer = "ad about it. record 179       ...."           
    *     
    *     The matching characters in the input
    *     buffer tail and delimiter head = "re" 
    *     Therefore, ambiguous byte count = 2 ****   //
    *     
    *     2.1 If the following bytes are the remaining characters of
    *         the delimiter, then we have to capture only up to the starting 
    *         position of delimiter. That means, we need not include the 
    *         ambiguous characters in str.
    *     
    *     2.2 If the following bytes are not the remaining characters of
    *         the delimiter ( as mentioned in the example ), 
    *         then we have to include the ambiguous characters in str. 
    */
    str.clear();
    int txtLength = 0; // tracks str.getLength(), as an optimization
    long bytesConsumed = 0;
    int delPosn = 0;
    int ambiguousByteCount=0; // To capture the ambiguous characters count
    do {
      int startPosn = bufferPosn; // Start from previous end position
      if (bufferPosn >= bufferLength) {
        startPosn = bufferPosn = 0;
        bufferLength = in.read(buffer);
        if (bufferLength <= 0) {
          str.append(recordDelimiterBytes, 0, ambiguousByteCount);
          break; // EOF
        }
      }
      for (; bufferPosn < bufferLength; ++bufferPosn) {
        if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
          delPosn++;
          if (delPosn >= recordDelimiterBytes.length) {
            bufferPosn++;
            break;
          }
        } else if (delPosn != 0) {
          bufferPosn--;
          delPosn = 0;
        }
      }
      int readLength = bufferPosn - startPosn;
      bytesConsumed += readLength;
      int appendLength = readLength - delPosn;
      if (appendLength > maxLineLength - txtLength) {
        appendLength = maxLineLength - txtLength;
      }
      if (appendLength > 0) {
        if (ambiguousByteCount > 0) {
          str.append(recordDelimiterBytes, 0, ambiguousByteCount);
          //appending the ambiguous characters (refer case 2.2)
          bytesConsumed += ambiguousByteCount;
          ambiguousByteCount=0;
        }
        str.append(buffer, startPosn, appendLength);
        txtLength += appendLength;
      }
      if (bufferPosn >= bufferLength) {
        if (delPosn > 0 && delPosn < recordDelimiterBytes.length) {
          ambiguousByteCount = delPosn;
          bytesConsumed -= ambiguousByteCount; //to be consumed in next
        }
      }
    } while (delPosn < recordDelimiterBytes.length 
        && bytesConsumed < maxBytesToConsume);
    if (bytesConsumed > (long) Integer.MAX_VALUE) {
      throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
    }
    return (int) bytesConsumed; 
  }

  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @param maxLineLength the maximum number of bytes to store into str.
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str, int maxLineLength) throws IOException {
    return readLine(str, maxLineLength, Integer.MAX_VALUE);
  }

  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str) throws IOException {
    return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
  }
}


}
