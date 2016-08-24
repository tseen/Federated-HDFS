package ncku.hpds.hadoop.fedhdfs;

public class TopCloudSlectionTest {
	
	public static void main(String args[]){
		String globalfileInput = args[0];
		TopcloudSelector top;
		String realTop = "";
		try {
			top = new TopcloudSelector(globalfileInput, false);
			realTop = top.getTopCloud();

		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Top Cloud of FedMR:" + realTop);
		
	}

}
