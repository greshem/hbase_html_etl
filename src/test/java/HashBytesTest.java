import org.apache.hadoop.hbase.util.Bytes;

import com.petty.enlp.EWord;
import com.petty.enlp.NLPService;
import com.petty.enlp.SegmentResult;

public class HashBytesTest {

	static int mod = 200;

	public static void main(String[] args) throws Exception {
		String originalKey = "zhihu_1460359153_b0b64812b500950178e269b582eb7143";
		byte[] prefix = new byte[256];
		prefix = getHashPrefix(originalKey.getBytes());
		prefix = Bytes.add(getHashPrefix(originalKey.getBytes()), originalKey.getBytes());
		System.out.println((int) 'a');
		System.out.println(prefix[0]);
		
		
		System.out.println(NLPService.check());
		String text = "";
		SegmentResult segmentResult = NLPService.getWords(text);
		for (EWord w : segmentResult.wordList) {
			System.out.println(w.word + "/" + w.nature.toString() + " ");
		}

	}

	public static byte[] getHashPrefix(byte[] originalKey) {
		long hash = Math.abs(hashBytes(originalKey));
		return new byte[] { (byte) (hash % mod) };
	}

	/** Compute hash for binary data. */
	private static int hashBytes(byte[] bytes) {
		int hash = 1;
		for (int i = 0; i < bytes.length; i++) {

			hash = (31 * hash) + (int) bytes[i];
		}
		return hash;
	}
}
