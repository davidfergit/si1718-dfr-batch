package data.streaming.dto;

public class PatentDTO {

	private String firstPatent;
	private String secondPatent;
	private Integer rating;
	
	public PatentDTO() {
		
	}

	public PatentDTO(String firstPatent, String secondPatent, Integer rating) {
		super();
		this.firstPatent = firstPatent;
		this.secondPatent = secondPatent;
		this.rating = rating;
	}

	public String getFirstPatent() {
		return firstPatent;
	}

	public void setFirstPatent(String firstPatent) {
		this.firstPatent = firstPatent;
	}

	public String getSecondPatent() {
		return secondPatent;
	}

	public void setSecondPatent(String secondPatent) {
		this.secondPatent = secondPatent;
	}

	public Integer getRating() {
		return rating;
	}

	public void setRating(Integer rating) {
		this.rating = rating;
	}

	@Override
	public String toString() {
		return "PatentDTO [firstPatent=" + firstPatent + ", secondPatent=" + secondPatent + ", rating=" + rating + "]";
	}
	
}
