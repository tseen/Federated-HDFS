package ncku.hpds.fed.MRv2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class InterPathFilter implements PathFilter {

	@Override
	public boolean accept(Path p) {
		String name = p.getName();
		return !name.startsWith("_") && !name.startsWith(".")
				&& !name.startsWith("p");
	}

}
