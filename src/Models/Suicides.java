package Models;

public class Suicides {
	
	private String state;
	private int year;
	private String type_major;
	private String type_minor;
	private String gender;
	private String age_group;
	private int total;
	
	public Suicides(String state, int year, String type_major, String type_minor,String gender, String age_group,
			int total) {
		super();
		this.state = state;
		this.year = year;
		this.type_major = type_major;
		this.type_minor = type_minor;
		this.gender = gender;
		this.age_group = age_group;
		this.total = total;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public String getType_major() {
		return type_major;
	}

	public void setType_major(String type_major) {
		this.type_major = type_major;
	}

	public String getType_minor() {
		return type_minor;
	}

	public void setType_minor(String type_minor) {
		this.type_minor = type_minor;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getAge_group() {
		return age_group;
	}

	public void setAge_group(String age_group) {
		this.age_group = age_group;
	}

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	@Override
	public String toString() {
		return "Suicides [state=" + state + ", year=" + year + ", type_major=" + type_major + ", type_minor="
				+ type_minor + ", gender=" + gender + ", age_group=" + age_group + ", total=" + total + "]";
	}
	

}
