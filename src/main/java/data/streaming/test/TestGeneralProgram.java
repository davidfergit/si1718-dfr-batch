package data.streaming.test;

public class TestGeneralProgram {

	public static void main(String[] args) {
		
		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					TestFlinkKafkaProducer.main(args);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}     
			}
		});
		t1.start();
		
		Thread t2 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					TestFlinkKafkaConsumer.main(args);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}      
			}
		});
		t2.start();
		
		Thread t3 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					TestBatch.main(args);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}     
			}
		});
		t3.start();

	}

}
